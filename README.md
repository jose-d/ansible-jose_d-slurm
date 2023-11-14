# Ansible Collection - jose_d.slurm

Interact with slurm instance - drain nodes, read their states,etc.

Install with `ansible-galaxy collection install jose_d.slurm`, published in [Ansible Galaxy](https://galaxy.ansible.com/ui/repo/published/jose_d/slurm/).

## Use

### slurm_scontrol

Example use:

#### Example 1 - drain node n2 with reason "Node maintenance"

```yaml
- name: Drain node
  jose_d.slurm.slurm_scontrol:
    nodes:
      - n2
    new_state: DRAIN
    new_state_reason: Node maintenance
  register: sc
  delegate_to: slurm1.cluster.lan

```

![Pylint](https://github.com/jose-d/ansible-jose_d-slurm/actions/workflows/pylint.yml/badge.svg)
