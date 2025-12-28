
# Déploiement Automatisé de Cluster Hadoop & Spark

Ce dépôt contient les scripts et playbooks Ansible nécessaires au déploiement, à la configuration et à la gestion d'un cluster Big Data distribué comprenant **Apache Hadoop** (HDFS, YARN) et **Apache Spark**.

## Architecture du Projet

Le projet est structuré selon les bonnes pratiques Ansible, séparant l'inventaire, les variables et les rôles.

```text
.
├── ansible
│   ├── ansible.cfg
│   ├── archives
│   ├── group_vars
│   │   ├── all.yaml             # Variables globales (Versions, Chemins, IPs)
│   │   ├── datanodes.yaml
│   │   └── namenode.yaml
│   ├── host_vars
│   │   ├── master.yaml
│   │   ├── worker1.yaml
│   │   ├── worker2.yaml
│   │   └── worker3.yaml
│   ├── inventory.yaml           # Inventaire des machines cibles
│   ├── playbook-cluster-setup.yaml  # Playbook principal
│   ├── playbook-debug-hadoop.yaml
│   ├── playbook-reset.yaml
│   ├── roles
│   │   ├── hadoop               # Installation et config HDFS/YARN
│   │   ├── java                 # Installation OpenJDK
│   │   ├── spark                # Installation Spark (Standalone/YARN)
│   │   ├── system_prep          # Configuration OS, Utilisateurs, Firewall
│   │   └── validation           # Tests de bon fonctionnement
│   └── test-ssh-connectivity.yaml
└── generate_passeword.py        # Utilitaire de hachage de mot de passe

```

## Prérequis Système

Pour garantir le bon fonctionnement des services distribués, chaque nœud (Master et Workers) doit respecter les spécifications suivantes :

* **Système d'exploitation :** Ubuntu (LTS recommandé)
* **Mémoire RAM :** > 4 Go par machine
* **Stockage :** > 35 Go d'espace disque disponible
* **Réseau :** Les machines doivent être accessibles via SSH depuis le poste de contrôle.

## Configuration Initiale

Avant de lancer le déploiement, suivez ces étapes de préparation :

### 1. Configuration SSH

Assurez-vous que la machine exécutant Ansible possède un accès SSH aux nœuds cibles. Il est recommandé de copier votre clé publique sur chaque serveur :

```bash
ssh-copy-id utilisateur@ip_serveur

```

### 2. Définition de l'Inventaire

Modifiez le fichier `ansible/inventory.yaml` pour y renseigner les adresses IP ou noms de domaine de vos machines (Master et Workers).

### 3. Variables Globales

Ajustez les variables de configuration dans `ansible/group_vars/all.yaml`. C'est ici que vous définissez :

* Les versions de Hadoop et Spark.
* Les chemins d'installation.
* Les sous-réseaux autorisés.

### 4. Sécurité (Mot de passe)

Générez le mot de passe chiffré pour l'utilisateur système `hadoop` qui sera créé sur le cluster.
Utilisez le script Python fourni :

```bash
python3 generate_passeword.py

```

Copiez le résultat (hash) dans la variable correspondante du fichier `ansible/group_vars/all.yaml`.

## Utilisation

Toutes les commandes doivent être exécutées depuis le dossier `ansible`.

### Déploiement Complet

Pour installer et configurer l'ensemble du cluster (Système, Java, Hadoop, Spark) :

```bash
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml -K

```

*L'option `-K` demandera le mot de passe `sudo` pour l'élévation de privilèges sur les machines distantes.*

### Déploiement Partiel (Tags)

Vous pouvez limiter l'exécution à certains rôles spécifiques grâce aux tags Ansible :

* **Système uniquement** (Utilisateurs, Hosts, Firewall) :
```bash
ansible-playbook -i inventory.yaml playbook-cluster-setup.yaml --tags system -K

```


* **Autres tags disponibles :**
* `network` : Configuration réseau spécifique.
* `java` : Installation de l'environnement Java.
* `hadoop` : Installation et configuration de HDFS et YARN.
* `spark` : Installation et configuration de Spark.



### Vérification

Pour tester la connectivité avant le déploiement :

```bash
ansible-playbook -i inventory.yaml test-ssh-connectivity.yaml

```
