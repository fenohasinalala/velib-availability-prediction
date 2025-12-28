# Ansible Playbook - Cluster Hadoop/Spark ESIROI

## Prérequis
```bash
# Sur votre machine de contrôle
sudo apt install ansible python3-apt

# Générer clé SSH
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```

## Utilisation rapide
```bash
# 1. Modifier inventory.yaml avec vos IPs
nano inventory.yaml

# 2. Lancer déploiement complet
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml

# 3. Lancer une phase spécifique
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml --tags hadoop

# 4. Mode test (dry-run)
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml --check

# 5. Reset cluster si problème
ansible-playbook -i inventory.yaml playbook-reset.yaml
```

## Tags disponibles
- `system` : Préparation système
- `java` : Installation Java
- `hadoop` : Installation Hadoop
- `spark` : Installation Spark
- `validation` : Tests et rapport

## Debugging
```bash
# Vérifier connectivité
ansible -i inventory.yaml all -m ping

# Voir logs détaillés
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml -vvv

# Vérifier services
ssh master "jps"
ssh worker1 "jps"
```

## Documentation
- GUIDE : docs/INSTALL.md
- TROUBLESHOOTING : docs/TROUBLESHOOTING.md
  