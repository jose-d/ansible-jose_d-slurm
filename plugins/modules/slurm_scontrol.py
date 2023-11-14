#!/usr/bin/env python3
#
# Author: Josef Dvoracek - <jose@fzu.cz> or <jose@dvojo.cz>
#
# Module to interact with slurm controller using scontrol

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common import yaml

DOCUMENTATION = r'''
---
module: slurm_scontrol

short_description: Module interacting with slurmctld using scontrol

version_added: "0.0.1"

description: The slurm_scontrol module provides the capability 
to modify or retrieve the state of a node within the Slurm workload 
manager through the utilization of the scontrol command.

options:
    nodes:
        description: List of nodes for the purpose of reading or modifying their states
        required: true
        type: list
    new_state:
        description:
            - If specified, new_state will be configured at nodes.
            - available states: DOWN,DRAIN,FAIL,FUTURE,NORESP,POWER_DOWN,POWER_DOWN_ASAP,POWER_DOWN_FORCE,POWER_UP,RESUME,UNDRAIN
        required: false
        type: string
    new_state_reason:
        description:
            - Reason to be used together with "drain" state
        required: false

author: Josef Dvoracek (@jose-d)
'''

EXAMPLES = r'''
# Read state of node
- name: Read state of node n2 and n3 and register it into nodes_state variable
  slurm_scontrol:
    nodes:
      - n2
      - n3
  register: nodes_state
  delegate_to: slurmserver.url

# Drain nodes with reason
- name: Drain nodes n2 and n3 and register their resulting state into nodes_state variable
  slurm_scontrol:
    nodes:
      - n2
      - n3
    new_state: DRAIN
    new_state_reason: Ansible testing
  register: nodes_state
  delegate_to: slurmserver.url

'''

RETURN = r'''
data:
    description: output of scontrol show command
    type: dict
    returned: always
    sample:
        "n2": {
                "active_features": [],
                "address": "n2.phoebe.lan",
                "alloc_cpus": 0,
                "alloc_idle_cpus": 128,
                ...
               }
reason_changed:
    description: If Reason was changed
    type: bool
    returned: always
    sample:
        "reason_changed": true

scontrol_commands:
    description: List of scontrol commands which were used to change state of target node
    type: list
    returned: always
    sample:
        "scontrol_commands": [
            "scontrol update node=n2 state=DRAIN reason=\"Code testing25S\""
        ],

scontrol_update_ran:
    description: If scontrol update command was ran by module
    type: bool
    returned: always
    sample:
        "scontrol_update_ran": true,


state_changed:
    description: If State was changed
    type: bool
    returned: always
    sample:
        "state_changed": false
'''

# constants:
NODE_ALLOWED_STATES=['DOWN','DRAIN','FAIL','FUTURE','NORESP','POWER_DOWN','POWER_DOWN_ASAP','POWER_DOWN_FORCE','POWER_UP','RESUME','UNDRAIN']

def run_module():
    
    # arguments/parameters:
    module_args = dict(
        nodes=dict(type='list', required=True, default=None),
        new_state=dict(type='str', required=False),
        new_state_reason=dict(type='str', required=False, default=None)
    )

    # RESULTS:
    result = dict(
        changed=False,
        state_changed=False,
        reason_changed=False,
        scontrol_commands=[],
        data='',
        scontrol_update_ran=False
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # Sanity checking:

    if ( module.params['new_state'] ):

        # * if new state must be in NODE_ALLOWED_STATES
        try:
            assert str(module.params['new_state']).upper() in NODE_ALLOWED_STATES
        except:
            module.fail_json(msg=f"new_state is not in {NODE_ALLOWED_STATES}", **result)
        
        # * when changing state to drain, we need reason
        if ( str(module.params['new_state']).upper() == 'DRAIN' ):
            try: 
                assert len(str(module.params['new_state_reason']))>1
                assert module.params['new_state_reason'] != None
            except: 
                module.fail_json(msg=f"If next state if drain, we need 'new_state_reason' argument to be specified.", **result)

    # verify if slurm controller is alive
    scontrolPing(module,result)

    # Collect current node state using scontrol - if we have more than single
    if ( len(module.params['nodes']) > 0 ):
        nodes_1 = collectNodesStatus(module.params['nodes'],module,result)
    else:
        module.fail_json(msg=f"No nodes provided, that's unexpeted.", **result)

    if ( len(module.params['nodes']) > 0 ) and ( module.params['new_state'] ):

        for node in module.params['nodes']:

            result['state_changed'] = not ( str(module.params['new_state']).upper() in nodes_1[node]['state'] )
            result['reason_changed'] = not ( str(module.params['new_state_reason']) == nodes_1[node]['reason'] )

            # If the node is already drained and reason is same, no need to do anything
            if not result['state_changed'] and not result['reason_changed']: continue

            result['scontrol_update_ran'] = True

            try:
                scontrol_command = f"scontrol update node={node} state={module.params['new_state']} reason=\"{module.params['new_state_reason']}\""
                result['scontrol_commands'].append(scontrol_command)
                if not module.check_mode:
                    scontrol_out = module.run_command(scontrol_command)
            except:
                module.fail_json(msg=f"Calling {scontrol_command} failed", **result)

    if result['scontrol_update_ran']:
        nodes_2 = collectNodesStatus(module.params['nodes'],module,result)
        result['data'] = nodes_2
    else:
        result['data'] = nodes_1

    #compile informations if changed:

    result['changed'] = result['scontrol_update_ran']
    
    module.exit_json(**result)


def scontrolPing(module,result):
    """" tests if we have working scontrol"""

    try:
        scontrol_command = f"scontrol ping"
        module.run_command(scontrol_command)
    except:
        module.fail_json(msg=f"Calling {scontrol_command} failed", **result)

def collectNodesStatus(nodes,module,result):
    """ run `scontrol show status` over nodes and returns it as a dict"""

    nodes_data = {}

    for node in nodes:
        try:
            scontrol_command = f"scontrol --yaml show node={node}"
            scontrol_out = module.run_command(scontrol_command)
        except:
            module.fail_json(msg=f"Calling {scontrol_command} failed", **result)

        scontrol_respond_yaml = yaml.yaml_load(scontrol_out[1])
        nodes_data[node] = scontrol_respond_yaml['nodes'][0]

    return nodes_data


def main():
    run_module()

if __name__ == '__main__':
    main()