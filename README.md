# Ansible Collection - jose_d.slurm

Interact with slurm instance - drain nodes, read their states,etc.


## `slurm_scontrol`

Example use:

### Example 1 - drain node n2 with reason "Node maintenance"

```
- name: Test my own module
  jose_d.slurm.slurm_scontrol:
    nodes:
      - n2
    new_state: DRAIN
    new_state_reason: Node maintenance
  register: sc
  delegate_to: slurm1.cluster.lan

```
