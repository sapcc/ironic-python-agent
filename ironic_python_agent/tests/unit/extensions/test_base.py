# Copyright 2013 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading
from unittest import mock

from stevedore import extension

from ironic_python_agent import errors
from ironic_python_agent.extensions import base
from ironic_python_agent.tests.unit import base as test_base


def _fake_validator(ext, **kwargs):
    if not kwargs.get('is_valid', True):
        raise errors.InvalidCommandParamsError('error')


class ExecutionError(errors.RESTError):
    def __init__(self):
        super(ExecutionError, self).__init__('failed')


class FakeExtension(base.BaseAgentExtension):
    @base.async_command('fake_async_command', _fake_validator)
    def fake_async_command(self, is_valid=False, param=None):
        if param == 'v2':
            raise ExecutionError()
        return param

    @base.sync_command('fake_sync_command', _fake_validator)
    def fake_sync_command(self, is_valid=False, param=None):
        if param == 'v2':
            raise ExecutionError()
        return param

    @base.async_command('other_async_name')
    def second_async_command(self):
        pass

    @base.sync_command('other_sync_name')
    def second_sync_command(self):
        pass


class AdmissionProbeExtension(base.BaseAgentExtension):
    """Extension that records the history at the moment it starts."""

    def __init__(self, agent=None):
        super(AdmissionProbeExtension, self).__init__(agent=agent)
        self.admitted_at_start = None
        self.calls = 0
        # Deterministic gates for the nested body test. The worker parks
        # between start and parameter read. No sleep is used.
        self.freeze_entered = threading.Event()
        self.freeze_resume = threading.Event()

    @base.async_command('admission_probe')
    def admission_probe(self, param=None):
        self.calls += 1
        self.admitted_at_start = 'stable-id' in self.agent.command_results
        return param

    @base.sync_command('admission_sync_probe')
    def admission_sync_probe(self, param=None):
        self.calls += 1
        return param

    @base.async_command('admission_async_freeze')
    def admission_async_freeze(self, body=None):
        self.calls += 1
        self.freeze_entered.set()
        if not self.freeze_resume.wait(5):
            raise RuntimeError('Fixture did not release the worker')
        return {'value': body['value'], 'items': list(body['items'])}


class FakeAgent(base.ExecuteCommandMixin):
    def __init__(self):
        super(FakeAgent, self).__init__()
        self.ext_mgr = extension.ExtensionManager.make_test_instance(
            [extension.Extension('fake', None, FakeExtension,
                                 FakeExtension())])

    def get_command_result(self, result_id):
        # Mirror the public REST history GET of the real agent.
        try:
            return self.command_results[result_id]
        except KeyError:
            raise errors.RequestedObjectNotFoundError('Command Result',
                                                      result_id)


class TestExecuteCommandMixin(test_base.IronicAgentTest):
    def setUp(self):
        super(TestExecuteCommandMixin, self).setUp()
        self.agent = FakeAgent()

    def test_execute_command(self):
        do_something_impl = mock.Mock()
        fake_extension = FakeExtension()
        fake_extension.command_map['do_something'] = do_something_impl
        self.agent.ext_mgr = extension.ExtensionManager.make_test_instance(
            [extension.Extension('fake', None, FakeExtension, fake_extension)])

        self.agent.execute_command('fake.do_something', foo='bar')
        do_something_impl.assert_called_once_with(foo='bar')

    def test_execute_invalid_command(self):
        self.assertRaises(errors.InvalidCommandError,
                          self.agent.execute_command,
                          'do_something',
                          foo='bar')

    def test_execute_unknown_extension(self):
        self.assertRaises(errors.RequestedObjectNotFoundError,
                          self.agent.execute_command,
                          'do.something',
                          foo='bar')

    def test_execute_command_success(self):
        expected_result = base.SyncCommandResult('fake', None, True, None)
        fake_ext = self.agent.get_extension('fake')
        fake_ext.execute = mock.Mock()
        fake_ext.execute.return_value = expected_result
        result = self.agent.execute_command('fake.sleep',
                                            sleep_info={"time": 1})
        self.assertEqual(expected_result, result)

    def test_execute_command_invalid_content(self):
        fake_ext = self.agent.ext_mgr['fake'].obj
        fake_ext.execute = mock.Mock()
        fake_ext.execute.side_effect = errors.InvalidContentError('baz')
        self.assertRaises(errors.InvalidContentError,
                          self.agent.execute_command,
                          'fake.sleep', sleep_info={"time": 1})

    def test_execute_command_other_exception(self):
        fake_ext = self.agent.ext_mgr['fake'].obj
        fake_ext.execute = mock.Mock()
        exc = errors.CommandExecutionError('foo bar baz')
        fake_ext.execute.side_effect = exc
        result = self.agent.execute_command(
            'fake.sleep', sleep_info={"time": 1}
        )
        self.assertEqual(base.AgentCommandStatus.FAILED,
                         result.command_status)
        self.assertEqual(exc, result.command_error)

    def test_busy(self):
        fake_extension = FakeExtension()
        self.agent.ext_mgr = extension.ExtensionManager.make_test_instance(
            [extension.Extension('fake', None, FakeExtension, fake_extension)])

        self.agent.command_results = {
            'fake': base.BaseCommandResult('name', {})
        }
        self.assertRaises(errors.AgentIsBusy,
                          self.agent.execute_command, 'fake.fake_sync_command')


class TestCommandAdmission(test_base.IronicAgentTest):
    """Admission binds a stable command identity before a start.

    The conductor owns the command identity in agent-poll mode. The agent
    must record that identity before an asynchronous command starts, so a
    redelivery cannot start the effect a second time.
    """

    def setUp(self):
        super(TestCommandAdmission, self).setUp()
        self.agent = FakeAgent()
        self.probe = AdmissionProbeExtension(agent=self.agent)
        self.agent.ext_mgr = extension.ExtensionManager.make_test_instance(
            [extension.Extension('probe', None, AdmissionProbeExtension,
                                 self.probe)])

    def test_admits_stable_id_before_async_start(self):
        result = self.agent.admit_command(
            'stable-id', 'probe.admission_probe', param='v1')

        result.join()

        self.assertEqual('stable-id', result.id)
        self.assertTrue(self.probe.admitted_at_start)
        self.assertEqual(1, self.probe.calls)

    def test_repeated_id_with_same_params_is_not_reexecuted(self):
        first = self.agent.admit_command(
            'stable-id', 'probe.admission_probe', param='v1')
        first.join()

        second = self.agent.admit_command(
            'stable-id', 'probe.admission_probe', param='v1')

        self.assertIs(first, second)
        self.assertEqual(1, self.probe.calls)

    def test_changed_params_for_same_id_are_rejected(self):
        first = self.agent.admit_command(
            'stable-id', 'probe.admission_probe', param='v1')
        first.join()

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            'stable-id', 'probe.admission_probe', param='v2')
        self.assertEqual(1, self.probe.calls)

    def test_changed_name_for_same_id_is_rejected(self):
        first = self.agent.admit_command(
            'stable-id', 'probe.admission_probe', param='v1')
        first.join()

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            'stable-id', 'probe.admission_sync_probe', param='v1')
        self.assertEqual(1, self.probe.calls)

    def test_sync_command_uses_stable_id(self):
        result = self.agent.admit_command(
            'sync-id', 'probe.admission_sync_probe', param='v1')

        self.assertEqual('sync-id', result.id)
        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         result.command_status)

    def test_history_keeps_admitted_ids_for_process_lifetime(self):
        for index in range(257):
            self.agent.admit_command(
                'sync-%d' % index, 'probe.admission_sync_probe')

        expected = ['sync-%d' % index for index in range(257)]
        self.assertEqual(expected, list(self.agent.command_results))

    def test_duplicate_id_after_257_later_commands_returns_recorded_result(
            self):
        first = self.agent.admit_command(
            'stable-id', 'probe.admission_sync_probe', param='v1')
        first.join()

        later_results = []
        for index in range(257):
            later_results.append(self.agent.admit_command(
                'later-%d' % index, 'probe.admission_sync_probe',
                param='v%d' % index))

        # No artificial admission cap: every distinct command completes.
        for later in later_results:
            self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                             later.command_status)
        before = self.probe.calls
        self.assertEqual(258, before)

        # The process kept the stable identity. Redelivery returns the
        # recorded result and does not run the effect a second time.
        repeated = self.agent.admit_command(
            'stable-id', 'probe.admission_sync_probe', param='v1')

        self.assertIs(first, repeated)
        self.assertEqual(before, self.probe.calls)

    def test_changed_body_after_257_later_commands_still_conflicts(self):
        self.agent.admit_command(
            'stable-id', 'probe.admission_sync_probe', param='v1')
        for index in range(257):
            self.agent.admit_command(
                'later-%d' % index, 'probe.admission_sync_probe',
                param='v%d' % index)

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            'stable-id', 'probe.admission_sync_probe', param='CHANGED')
        self.assertEqual(258, self.probe.calls)

    def test_changed_name_after_257_later_commands_still_conflicts(self):
        self.agent.admit_command(
            'stable-id', 'probe.admission_sync_probe', param='v1')
        for index in range(257):
            self.agent.admit_command(
                'later-%d' % index, 'probe.admission_sync_probe',
                param='v%d' % index)

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            'stable-id', 'probe.admission_probe', param='v1')
        self.assertEqual(258, self.probe.calls)

    def test_legacy_history_survives_257_completed_commands(self):
        # Legacy REST path: execute_command has no stable identity and
        # generates one per result. The baseline lifecycle keeps every
        # recorded result for the process lifetime.
        first = self.agent.execute_command(
            'probe.admission_sync_probe', param='v1')

        for index in range(257):
            self.agent.execute_command(
                'probe.admission_sync_probe', param='v%d' % index)

        self.assertEqual(258, self.probe.calls)

        # Legacy retrieval uses the history GET by id. The first record
        # stays retrievable with its original status and result.
        recorded = self.agent.get_command_result(first.id)

        self.assertIs(first, recorded)
        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         recorded.command_status)
        self.assertEqual({'result': 'v1'}, recorded.command_result)
        self.assertEqual(258, self.probe.calls)

    def test_legacy_record_collision_same_body_is_rejected(self):
        legacy = self.agent.execute_command(
            'probe.admission_sync_probe', param='v1')
        self.assertIsNone(legacy.command_fingerprint)

        # The legacy record has no fingerprint. The agent cannot prove
        # a qualified binding, so it must fail closed. It must not
        # return the legacy result for the admitted identity.
        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            legacy.id, 'probe.admission_sync_probe', param='v1')

        self.assertEqual(1, self.probe.calls)
        self.assertIs(legacy, self.agent.get_command_result(legacy.id))
        self.assertEqual({'result': 'v1'}, legacy.command_result)

    def test_legacy_record_collision_changed_body_is_rejected(self):
        legacy = self.agent.execute_command(
            'probe.admission_sync_probe', param='v1')

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            legacy.id, 'probe.admission_sync_probe', param='v2')

        self.assertEqual(1, self.probe.calls)
        self.assertIs(legacy, self.agent.get_command_result(legacy.id))

    def test_legacy_record_collision_changed_name_is_rejected(self):
        legacy = self.agent.execute_command(
            'probe.admission_sync_probe', param='v1')

        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            legacy.id, 'probe.admission_probe', param='v1')

        self.assertEqual(1, self.probe.calls)
        self.assertIs(legacy, self.agent.get_command_result(legacy.id))

    def test_async_admission_detaches_nested_body_before_execution(self):
        body = {'value': 'original', 'items': [1, 2, 3]}
        result = self.agent.admit_command(
            'async-id', 'probe.admission_async_freeze', body=body)

        # The worker is in and parked. The caller mutates the caller-
        # owned nested dict and list after admission.
        self.assertTrue(self.probe.freeze_entered.wait(5),
                        'Worker did not start before the mutation')
        body['value'] = 'mutated'
        body['items'][0] = 99
        self.probe.freeze_resume.set()
        result.join(5)

        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         result.command_status)
        # The extension must execute the originally accepted values.
        self.assertEqual({'value': 'original', 'items': [1, 2, 3]},
                         result.command_result)

    def test_fingerprint_binds_original_body_after_caller_mutation(self):
        body = {'value': 'original', 'items': [1, 2, 3]}
        result = self.agent.admit_command(
            'async-id', 'probe.admission_async_freeze', body=body)
        self.assertTrue(self.probe.freeze_entered.wait(5),
                        'Worker did not start before the mutation')
        body['value'] = 'mutated'
        body['items'][0] = 99
        self.probe.freeze_resume.set()
        result.join(5)

        # The fingerprint still binds the original body. The same body in
        # any key order returns the recorded result without re-running.
        self.assertIs(result, self.agent.admit_command(
            'async-id', 'probe.admission_async_freeze',
            body={'value': 'original', 'items': [1, 2, 3]}))
        self.assertIs(result, self.agent.admit_command(
            'async-id', 'probe.admission_async_freeze',
            body={'items': [1, 2, 3], 'value': 'original'}))

        # The mutated body is a different frozen body for the identity.
        self.assertRaises(
            errors.AgentCommandConflict, self.agent.admit_command,
            'async-id', 'probe.admission_async_freeze', body=body)
        self.assertEqual(1, self.probe.calls)


class TestExtensionDecorators(test_base.IronicAgentTest):
    def setUp(self):
        super(TestExtensionDecorators, self).setUp()
        self.agent = FakeAgent()
        self.agent.force_heartbeat = mock.Mock()
        self.extension = FakeExtension(agent=self.agent)

    def test_async_command_success(self):
        result = self.extension.execute('fake_async_command', param='v1')
        self.assertIsInstance(result, base.AsyncCommandResult)
        result.join()
        self.assertEqual('fake_async_command', result.command_name)
        self.assertEqual({'param': 'v1'}, result.command_params)
        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         result.command_status)
        self.assertIsNone(result.command_error)
        self.assertEqual({'result': 'fake_async_command: v1'},
                         result.command_result)
        self.agent.force_heartbeat.assert_called_once_with()

    def test_wait_async_command_success(self):
        result = self.extension.execute('fake_async_command', param='v1')
        self.assertIsInstance(result, base.AsyncCommandResult)
        result = result.wait()
        self.assertEqual({'result': 'fake_async_command: v1'}, result)

    def test_async_command_success_without_agent(self):
        extension = FakeExtension(agent=None)
        result = extension.execute('fake_async_command', param='v1')
        self.assertIsInstance(result, base.AsyncCommandResult)
        result.join()
        self.assertEqual('fake_async_command', result.command_name)
        self.assertEqual({'param': 'v1'}, result.command_params)
        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         result.command_status)
        self.assertIsNone(result.command_error)
        self.assertEqual({'result': 'fake_async_command: v1'},
                         result.command_result)

    def test_async_command_validation_failure(self):
        self.assertRaises(errors.InvalidCommandParamsError,
                          self.extension.execute,
                          'fake_async_command',
                          is_valid=False)
        # validation is synchronous, no need to force a heartbeat
        self.assertEqual(0, self.agent.force_heartbeat.call_count)

    def test_async_command_execution_failure(self):
        result = self.extension.execute('fake_async_command', param='v2')
        self.assertIsInstance(result, base.AsyncCommandResult)
        result.join()
        self.assertEqual('fake_async_command', result.command_name)
        self.assertEqual({'param': 'v2'}, result.command_params)
        self.assertEqual(base.AgentCommandStatus.FAILED,
                         result.command_status)
        self.assertIsInstance(result.command_error, ExecutionError)
        self.assertIsNone(result.command_result)
        self.agent.force_heartbeat.assert_called_once_with()

    def test_wait_async_command_execution_failure(self):
        result = self.extension.execute('fake_async_command', param='v2')
        self.assertIsInstance(result, base.AsyncCommandResult)
        self.assertRaises(ExecutionError, result.wait)

    def test_async_command_name(self):
        self.assertEqual(
            'other_async_name',
            self.extension.second_async_command.command_name)

    def test_sync_command_success(self):
        result = self.extension.execute('fake_sync_command', param='v1')
        self.assertIsInstance(result, base.SyncCommandResult)
        self.assertEqual('fake_sync_command', result.command_name)
        self.assertEqual({'param': 'v1'}, result.command_params)
        self.assertEqual(base.AgentCommandStatus.SUCCEEDED,
                         result.command_status)
        self.assertIsNone(result.command_error)
        self.assertEqual({'result': 'v1'}, result.command_result)
        # no need to force heartbeat on a sync command
        self.assertEqual(0, self.agent.force_heartbeat.call_count)

    def test_sync_command_validation_failure(self):
        self.assertRaises(errors.InvalidCommandParamsError,
                          self.extension.execute,
                          'fake_sync_command',
                          is_valid=False)
        # validation is synchronous, no need to force a heartbeat
        self.assertEqual(0, self.agent.force_heartbeat.call_count)

    def test_sync_command_execution_failure(self):
        self.assertRaises(ExecutionError,
                          self.extension.execute,
                          'fake_sync_command',
                          param='v2')
        # no need to force heartbeat on a sync command
        self.assertEqual(0, self.agent.force_heartbeat.call_count)

    def test_sync_command_name(self):
        self.assertEqual(
            'other_sync_name',
            self.extension.second_sync_command.command_name)

    def test_command_map(self):
        expected_map = {
            'fake_async_command': self.extension.fake_async_command,
            'fake_sync_command': self.extension.fake_sync_command,
            'other_async_name': self.extension.second_async_command,
            'other_sync_name': self.extension.second_sync_command,
        }
        self.assertEqual(expected_map, self.extension.command_map)
