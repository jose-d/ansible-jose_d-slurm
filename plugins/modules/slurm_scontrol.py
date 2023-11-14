#!/usr/bin/env python3
#
# Author: Josef Dvoracek - <jose@fzu.cz> or <jose@dvojo.cz>
#
# Module to interact with slurm controller using scontrol

from __future__ import (absolute_import, division, print_function)
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common import yaml

DOCUMENTATION = """
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

EXAMPLES = """
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


# constants:
NODE_ALLOWED_STATES=['DOWN','DRAIN','FAIL','FUTURE','NORESP',\
    'POWER_DOWN','POWER_DOWN_ASAP','POWER_DOWN_FORCE','POWER_UP','RESUME','UNDRAIN']

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