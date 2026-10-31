# Copyright 2013 Rackspace, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import copy
import functools
import hashlib
import inspect
import json
import threading

from oslo_log import log
from oslo_utils import uuidutils
from stevedore import extension

from ironic_python_agent import encoding
from ironic_python_agent import errors
from ironic_python_agent import utils


LOG = log.getLogger()


class AgentCommandStatus(object):
    """Mapping of agent command statuses."""
    RUNNING = u'RUNNING'
    SUCCEEDED = u'SUCCEEDED'
    FAILED = u'FAILED'
    # TODO(dtantsur): keeping the same text for backward compatibility, change
    # to just VERSION_MISMATCH one release after ironic is updated.
    VERSION_MISMATCH = u'CLEAN_VERSION_MISMATCH'


class BaseCommandResult(encoding.SerializableComparable):
    """Base class for command result."""

    serializable_fields = ('id', 'command_name',
                           'command_status', 'command_error', 'command_result')

    def __init__(self, command_name, command_params, command_id=None,
                 command_fingerprint=None):
        """Construct an instance of BaseCommandResult.

        :param command_name: name of command executed
        :param command_params: parameters passed to command
        :param command_id: stable command identity owned by the caller. A
            generated identity is used when the caller does not have one.
        :param command_fingerprint: digest of the frozen name and parameters,
            used to detect a changed body for a stable identity. It is never
            serialized because it is derived from possibly secret values.
        """

        self.id = command_id or uuidutils.generate_uuid()
        self.command_name = command_name
        self.command_params = command_params
        self.command_fingerprint = command_fingerprint
        self.command_status = AgentCommandStatus.RUNNING
        self.command_error = None
        self.command_result = None

    def __str__(self):
        return ("Command name: %(name)s, "
                "params: %(params)s, status: %(status)s, result: "
                "%(result)s." %
                {"name": self.command_name,
                 "params": utils.remove_large_keys(self.command_params),
                 "status": self.command_status,
                 "result": utils.remove_large_keys(self.command_result)})

    def is_done(self):
        """Checks to see if command is still RUNNING.

        :returns: True if command is done, False if still RUNNING
        """
        return self.command_status != AgentCommandStatus.RUNNING

    def join(self):
        """:returns: result of completed command."""
        return self

    def wait(self):
        """Join the result and extract its value.

        Raises if the command failed.
        """
        self.join()
        if self.command_error is not None:
            raise self.command_error
        else:
            return self.command_result


class SyncCommandResult(BaseCommandResult):
    """A result from a command that executes synchronously."""

    def __init__(self, command_name, command_params, success, result_or_error,
                 command_id=None, command_fingerprint=None):
        """Construct an instance of SyncCommandResult.

        :param command_name: name of command executed
        :param command_params: parameters passed to command
        :param success: True indicates success, False indicates failure
        :param result_or_error: Contains the result (or error) from the command
        """

        super(SyncCommandResult, self).__init__(
            command_name, command_params, command_id=command_id,
            command_fingerprint=command_fingerprint)
        if isinstance(result_or_error, (bytes, str)):
            result_key = 'result' if success else 'error'
            result_or_error = {result_key: result_or_error}

        if success:
            self.command_status = AgentCommandStatus.SUCCEEDED
            self.command_result = result_or_error
        else:
            self.command_status = AgentCommandStatus.FAILED
            self.command_error = result_or_error


class AsyncCommandResult(BaseCommandResult):
    """A command that executes asynchronously in the background."""

    def __init__(self, command_name, command_params, execute_method,
                 agent=None, command_id=None, command_fingerprint=None):
        """Construct an instance of AsyncCommandResult.

        :param command_name: name of command to execute
        :param command_params: parameters passed to command
        :param execute_method: a callable to be executed asynchronously
        :param agent: Optional: an instance of IronicPythonAgent
        """
        super(AsyncCommandResult, self).__init__(
            command_name, command_params, command_id=command_id,
            command_fingerprint=command_fingerprint)
        self.agent = agent
        self.execute_method = execute_method
        self.command_state_lock = threading.Lock()

        thread_name = 'agent-command-{}'.format(self.id)
        self.execution_thread = threading.Thread(target=self.run,
                                                 name=thread_name)

    def serialize(self):
        """Serializes the AsyncCommandResult into a dict.

        :returns: dict containing serializable fields in AsyncCommandResult
        """
        with self.command_state_lock:
            return super(AsyncCommandResult, self).serialize()

    def start(self):
        """Begin background execution of command."""
        self.execution_thread.start()
        return self

    def join(self, timeout=None):
        """Block until command has completed, and return result.

        :param timeout: float indicating max seconds to wait for command
                        to complete. Defaults to None.
        """
        self.execution_thread.join(timeout)
        return self

    def is_done(self):
        """Checks to see if command is still RUNNING.

        :returns: True if command is done, False if still RUNNING
        """
        with self.command_state_lock:
            return super(AsyncCommandResult, self).is_done()

    def run(self):
        """Run a command."""
        try:
            result = self.execute_method(**self.command_params)

            if isinstance(result, (bytes, str)):
                result = {'result': '{}: {}'.format(self.command_name, result)}
            LOG.info('Asynchronous command %(name)s completed: %(result)s',
                     {'name': self.command_name,
                      'result': utils.remove_large_keys(result)})
            with self.command_state_lock:
                self.command_result = result
                self.command_status = AgentCommandStatus.SUCCEEDED
        except errors.VersionMismatch as e:
            with self.command_state_lock:
                self.command_error = e
                self.command_status = AgentCommandStatus.VERSION_MISMATCH
                self.command_result = None
            LOG.error('Clean version mismatch for command %s',
                      self.command_name)
        except Exception as e:
            LOG.exception('Command failed: %(name)s, error: %(err)s',
                          {'name': self.command_name, 'err': e})
            if not isinstance(e, errors.RESTError):
                e = errors.CommandExecutionError(str(e))

            with self.command_state_lock:
                self.command_error = e
                self.command_status = AgentCommandStatus.FAILED
        finally:
            if self.agent:
                self.agent.force_heartbeat()


class BaseAgentExtension(object):
    def __init__(self, agent=None):
        super(BaseAgentExtension, self).__init__()
        self.agent = agent
        self.command_map = dict(
            (v.command_name, v)
            for k, v in inspect.getmembers(self)
            if hasattr(v, 'command_name')
        )

    def execute(self, command_name, **kwargs):
        cmd = self.command_map.get(command_name)
        if cmd is None:
            raise errors.InvalidCommandError(
                'Unknown command: {}'.format(command_name))
        return cmd(**kwargs)

    def check_cmd_presence(self, ext_obj, ext, cmd):
        if not (hasattr(ext_obj, 'execute') and hasattr(ext_obj, 'command_map')
                and cmd in ext_obj.command_map):
            raise errors.InvalidCommandParamsError(
                "Extension {} doesn't provide {} method".format(ext, cmd))


class ExecuteCommandMixin(object):

    def __init__(self):
        # command_lock serializes command execution. command_state_lock
        # serializes the history so that a reporter snapshot never needs the
        # execution lock.
        self.command_lock = threading.Lock()
        self.command_state_lock = threading.Lock()
        self.command_results = collections.OrderedDict()
        self._admission = threading.local()
        self.ext_mgr = None

    def get_extension(self, extension_name):
        if self.ext_mgr is None:
            raise errors.ExtensionError('Extension manager is not initialized')
        ext = self.ext_mgr[extension_name].obj
        ext.ext_mgr = self.ext_mgr
        return ext

    def split_command(self, command_name):
        command_parts = command_name.split('.', 1)
        if len(command_parts) != 2:
            raise errors.InvalidCommandError(
                'Command name must be of the form <extension>.<name>')

        return (command_parts[0], command_parts[1])

    def _find_command_result(self, command_id):
        with self.command_state_lock:
            return self.command_results.get(command_id)

    def register_command_result(self, result):
        """Record an admitted command in the process-lifetime history.

        The caller must have admitted the identity already. An asynchronous
        result is registered before its execution thread starts.
        """
        with self.command_state_lock:
            self.command_results[result.id] = result

    def execute_command(self, command_name, **kwargs):
        """Execute an agent command."""
        with self.command_lock:
            return self._execute_command(command_name, **kwargs)

    def admit_command(self, command_id, command_name, **kwargs):
        """Execute one command under a caller-owned stable identity.

        In agent-poll mode the conductor owns the command identity. The
        identity is bound to the result and inserted into the history before
        an asynchronous command starts. A redelivery of the same identity and
        body returns the recorded result and does not execute a second time.
        A changed name or body for the same identity is rejected.
        """
        # Detach nested parameters as soon as the identity is accepted.
        # The fingerprint and the executed body must both describe the
        # values the caller submitted, not a later mutation of the same
        # objects. deepcopy keeps the accepted parameter types unchanged.
        command_params = copy.deepcopy(kwargs)
        fingerprint = _command_fingerprint(command_name, command_params)
        with self.command_lock:
            existing = self._find_command_result(command_id)
            if existing is not None:
                if existing.command_fingerprint != fingerprint:
                    raise errors.AgentCommandConflict(command_id)
                return existing

            self._admission.command_id = command_id
            self._admission.command_fingerprint = fingerprint
            try:
                return self._execute_command(command_name, **command_params)
            finally:
                self._admission.command_id = None
                self._admission.command_fingerprint = None

    def _execute_command(self, command_name, **kwargs):
        """Execute a command while the caller holds command_lock."""
        LOG.debug('Executing command: %(name)s with args: %(args)s',
                  {'name': command_name,
                   'args': utils.remove_large_keys(kwargs)})
        extension_part, command_part = self.split_command(command_name)

        if len(self.command_results) > 0:
            last_command = list(self.command_results.values())[-1]
            if not last_command.is_done():
                LOG.error('Tried to execute %(command)s, agent is still '
                          'executing %(last)s', {'command': command_name,
                                                 'last': last_command})
                raise errors.AgentIsBusy(last_command.command_name)

        try:
            ext = self.get_extension(extension_part)
            result = ext.execute(command_part, **kwargs)
        except KeyError:
            # Extension Not found
            LOG.exception('Extension %s not found', extension_part)
            raise errors.RequestedObjectNotFoundError('Extension',
                                                      extension_part)
        except errors.InvalidContentError as e:
            # Any command may raise a InvalidContentError which will be
            # returned to the caller directly.
            LOG.exception('Invalid content error: %s', e)
            raise e
        except Exception as e:
            # Other errors are considered command execution errors, and are
            # recorded as a failed SyncCommandResult with an error message
            LOG.exception('Command execution error: %s', e)
            result = SyncCommandResult(command_name, kwargs, False, e,
                                       **_pending_admission(self))
        self.register_command_result(result)
        return result


def _command_fingerprint(command_name, command_params):
    """Digest the frozen command name and parameters.

    The digest must not depend on dict ordering, because the same frozen
    body arrives in whatever order the transport produced. The digest is
    never logged, because parameters may carry secrets.
    """
    payload = json.dumps([command_name, command_params], sort_keys=True,
                         separators=(',', ':'), default=repr)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _pending_admission(mixin):
    admission = getattr(mixin, '_admission', None)
    if admission is None:
        return {}
    command_id = getattr(admission, 'command_id', None)
    if command_id is None:
        return {}
    return {'command_id': command_id,
            'command_fingerprint': getattr(
                admission, 'command_fingerprint', None)}


def async_command(command_name, validator=None):
    """Will run the command in an AsyncCommandResult in its own thread.

    command_name is set based on the func name and command_params will
    be whatever args/kwargs you pass into the decorated command.
    Return values of type `str` or `unicode` are prefixed with the
    `command_name` parameter when returned for consistency.
    """
    def async_decorator(func):
        func.command_name = command_name

        @functools.wraps(func)
        def wrapper(self, **command_params):
            # Run a validator before passing everything off to async.
            # validators should raise exceptions or return silently.
            if validator:
                validator(self, **command_params)

            # bind self to func so that AsyncCommandResult doesn't need to
            # know about the mode
            bound_func = functools.partial(func, self)

            admission = _pending_admission(self.agent)
            ret = AsyncCommandResult(command_name,
                                     command_params,
                                     bound_func,
                                     agent=self.agent,
                                     **admission)
            # Bind an admitted identity into the history before the
            # execution thread starts: a redelivery must not start the
            # same effect twice. Commands without a caller-owned identity
            # are NOT bound here. Direct-http mode records the top-level
            # command when the API call returns, and a command started
            # from inside another command's thread (e.g. prepare_image
            # inside write_image) must stay out of the conductor-visible
            # history, otherwise the conductor's last-command correlation
            # can never match the deploy step again.
            if admission:
                self.agent.register_command_result(ret)
            ret.start()
            LOG.info('Asynchronous command %(name)s started execution',
                     {'name': command_name})
            return ret
        return wrapper
    return async_decorator


def sync_command(command_name, validator=None):
    """Decorate a method to wrap its return value in a SyncCommandResult.

    For consistency with @async_command() can also accept a
    validator which will be used to validate input, although a synchronous
    command can also choose to implement validation inline.
    """
    def sync_decorator(func):
        func.command_name = command_name

        @functools.wraps(func)
        def wrapper(self, **command_params):
            # Run a validator before invoking the function.
            # validators should raise exceptions or return silently.
            if validator:
                validator(self, **command_params)

            result = func(self, **command_params)
            LOG.info('Synchronous command %(name)s completed: %(result)s',
                     {'name': command_name,
                      'result': utils.remove_large_keys(result)})
            return SyncCommandResult(command_name,
                                     command_params,
                                     True,
                                     result,
                                     **_pending_admission(self.agent))

        return wrapper
    return sync_decorator


_EXT_MANAGER = None


def init_ext_manager(agent):
    global _EXT_MANAGER
    _EXT_MANAGER = extension.ExtensionManager(
        namespace='ironic_python_agent.extensions',
        invoke_on_load=True,
        propagate_map_exceptions=True,
        invoke_kwds={'agent': agent},
    )
    return _EXT_MANAGER


def get_extension(name):
    if _EXT_MANAGER is None:
        raise errors.ExtensionError('Extension manager is not initialized')
    ext = _EXT_MANAGER[name].obj
    ext.ext_mgr = _EXT_MANAGER
    return ext
