# Rôle system_prep

## Description
Prépare le système avant installation Hadoop/Spark :
- Update & upgrade Ubuntu
- Installation sudo
- Création utilisateur hadoop avec mot de passe
- Configuration SSH passwordless
- Création répertoires de base (/data)

## Utilisation
```bash
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml --tags system
```

## Variables
- `hadoop_user` : Nom de l'utilisateur (défaut: hadoop)
- `hadoop_user_password` : Hash du mot de passe SHA-512
- `data_root` : Répertoire racine données (défaut: /data/hdfs)
- `system_packages` : Liste paquets à installer

## Sécurité
** IMPORTANT ** : Changez le mot de passe par défaut avant production !
```bash
python3 -c "import crypt; print(crypt.crypt('VOTRE_MDP', crypt.mksalt(crypt.METHOD_SHA512)))"
```
