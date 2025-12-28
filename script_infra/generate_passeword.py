#!/usr/bin/env python3
"""
Script pour générer un hash SHA-512 de mot de passe compatible Linux/shadow
"""

import sys
import getpass
from passlib.hash import sha512_crypt

def main():
    """Génère un hash SHA-512 d'un mot de passe saisi de manière sécurisée"""
    
    print("=== Générateur de Hash SHA-512 ===\n")
    
    try:
        # Demande le mot de passe de manière sécurisée (sans affichage)
        password = getpass.getpass("Entrez le mot de passe : ")
        
        if not password:
            print("Erreur : Le mot de passe ne peut pas être vide.", file=sys.stderr)
            sys.exit(1)
        
        # Confirmation du mot de passe
        password_confirm = getpass.getpass("Confirmez le mot de passe : ")
        
        if password != password_confirm:
            print("Erreur : Les mots de passe ne correspondent pas.", file=sys.stderr)
            sys.exit(1)
        
        # Génération du hash
        print("\nGénération du hash en cours...")
        password_hash = sha512_crypt.hash(password)
        
        # Affichage du résultat
        print("\n" + "="*70)
        print("Hash généré :")
        print("="*70)
        print(password_hash)
        print("="*70)
        print("\nCe hash peut être utilisé dans /etc/shadow")
        
    except KeyboardInterrupt:
        print("\n\nOpération annulée.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nErreur : {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()