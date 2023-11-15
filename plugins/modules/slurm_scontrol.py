"""
    This module serves as an implementation of an Ansible module, 
    encapsulating fundamental scontrol commands for seamless integration into Ansible playbooks.
"""
#!/usr/bin/env python3
#
# Author: Josef Dvoracek - <jose@fzu.cz> or <jose@dvojo.cz>
#
# Module to interact with slurm controller using scontrol

from __future__ import (absolute_import, division, print_function)
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common import yaml

DOCUMENTATION = r"""
---
module: slurm_scontrol
version_added: "0.0.4"
author: Josef Dvoracek (@jose-d)
short_description: Module interacting with slurmctld using scontrol
description:
  - The slurm_scontrol module provides the capability
  - to modify or retrieve the state of a node within the Slurm workload
  - manager through the utilization of the scontrol command.
options:
    nodes:
        description:
            - List of nodes for the purpose of reading or modifying their states.
        required: true
        type: list
        elements: str
    new_state:
        description:
            - If specified, new_state will be configured at nodes.
            - available states DOWN,DRAIN,FAIL,FUTURE,NORESP,POWER_DOWN,POWER_DOWN_ASAP,POWER_DOWN_FORCE,POWER_UP,RESUME,UNDRAIN.
        required: false
        type: str
    new_state_reason:
        description:
            - Reason to be used together with "drain" state
            - eg. 'node maintenance'
        required: false
        type: str
"""

EXAMPLES = r"""
- name: Read state of node n[2,3] and register result into nodes_state
  jose_d.slurm.slurm_scontrol:
    nodes:
      - n2
      - n3
  register: nodes_state
  delegate_to: slurmserver.url

- name: Drain nodes n2 and n3 and register resulting state into nodes_state
  jose_d.slurm.slurm_scontrol:
    nodes:
      - n2
      - n3
    new_state: DRAIN
    new_state_reason: Ansible testing
  register: nodes_state
  delegate_to: slurmserver.url
"""

RETURN = r"""
data:
    description: The YAML output obtained from the scontrol command parsed into a dict.
    type: dict
    returned: always
reason_changed:
    description: If the Reason attribute of a node has been modified, this variable will be set to True.
    type: bool
    returned: always
state_changed:
    description: This variable is True when the State attribute of a node has been modified.
    type: bool
    returned: always
scontrol_commands:
    description: List of scontrol commands used to change state of target node
    type: list
    returned: always
scontrol_update_ran:
    description: If scontrol update command was ran by module
    type: bool
    returned: always
"""


# constants:
NODE_ALLOWED_STATES=['DOWN','DRAIN','FAIL','FUTURE','NORESP',\
    'POWER_DOWN','POWER_DOWN_ASAP','POWER_DOWN_FORCE','POWER_UP','RESUME','UNDRAIN']

STATES_NEED_REASON=['DRAIN']

def sanitize_input(module,result):
    """Sanitization of module arguments"""

    # verify if state is allowed
    if (
        ( module.params['new_state'] ) and
        ( str(module.params['new_state']).upper() not in NODE_ALLOWED_STATES )
    ):
        module.fail_json(msg=f"new_state is not in {NODE_ALLOWED_STATES}", **result)

    # if draining, we need Reason
    if (
        ( str(module.params['new_state']).upper() in STATES_NEED_REASON ) and
        ( module.params['new_state_reason'] is None )
    ):
        module.fail_json(msg=f"If next state is in {STATES_NEED_REASON}, \
            we need 'new_state_reason' argument to be specified.", **result)

    # we need at least one node
    if not len(module.params['nodes']) > 0:
        module.fail_json(msg="No nodes provided, that's unexpeted.", **result)


def run_module():
    """Main logic of module is here"""

    # arguments/parameters:
    module_args = {
        "nodes": {
            "type": list,
            "required": True,
            "default": None
        },
        "new_state": {
            "type": str,
            "required": False
        },
        "new_state_reason": {
            "type": str,
            "required": False,
            "default": None
        }
    }

    # RESULTS:
    result = {
        'changed': False,
        'state_changed': False,
        'reason_changed': False,
        'scontrol_commands': [],
        'data': '',
        'scontrol_update_ran': False
    }

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # Sanity checking:
    sanitize_input(module,result)

    # verify if slurm controller is alive
    scontrol_ping(module)

    nodes = module.params['nodes']

    nodes_1 = collect_nodes_status(nodes,module)

    if module.params['new_state']:

        new_state = str(module.params['new_state']).upper()
        new_state_reason = str(module.params['new_state_reason'])

        for node in nodes:

            result['state_changed'] = \
                new_state not in nodes_1[node]['state']
            result['reason_changed'] = \
                not new_state_reason == nodes_1[node]['reason']

            # If the node is already drained and reason is same, no need to do anything
            if not result['state_changed'] and not result['reason_changed']:
                continue

            result['scontrol_update_ran'] = True

            scontrol_command = \
                f"scontrol update node={node} state={new_state} reason=\"{new_state_reason}\""
            result['scontrol_commands'].append(scontrol_command)
            if not module.check_mode:
                res = module.run_command(scontrol_command)
                if res[0] != 0:
                    module.fail_json( \
                        msg=f"Calling {scontrol_command} returned non-zero RC", \
                        **result)

    if result['scontrol_update_ran']:
        nodes_2 = collect_nodes_status(nodes,module)
        result['data'] = nodes_2
    else:
        result['data'] = nodes_1

    #compile informations if changed:

    result['changed'] = result['scontrol_update_ran']
    module.exit_json(**result)


def scontrol_ping(module):
    """" Tests if we have working scontrol"""

    scontrol_command = "scontrol ping"
    module.run_command(scontrol_command)

def collect_nodes_status(nodes,module):
    """ Run `scontrol show status` over nodes and returns it as a dict"""

    nodes_data = {}

    for node in nodes:
        scontrol_command = f"scontrol --yaml show node={node}"
        scontrol_out = module.run_command(scontrol_command)

        scontrol_respond_yaml = yaml.yaml_load(scontrol_out[1])
        nodes_data[node] = scontrol_respond_yaml['nodes'][0]

    return nodes_data


def main():
    """wrapper for run_module() - described at Ansible documentation"""
    run_module()

if __name__ == '__main__':
    main()
