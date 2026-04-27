# Procédure SSL — `beta.scorenzer.com` et `beta.serenzer.com`

**Pour** : Noé
**Serveur** : `67.205.154.40`
**Pré-requis** : accès root SSH, Nginx déjà installé, Certbot déjà installé (utilisé pour `feedback.noejoraz.org`)

---

## Contexte

Les domaines `beta.scorenzer.com` et `beta.serenzer.com` pointent désormais vers ton serveur (`67.205.154.40`). Ils doivent servir la **même application** que celle exposée sur `feedback.noejoraz.org` (API d'import des invitations).

L'objectif : ajouter ces deux domaines à la config Nginx existante et générer les certificats Let's Encrypt correspondants.

---

## 1. Localiser la config Nginx existante

```bash
sudo grep -rln "feedback.noejoraz.org" /etc/nginx/
```

Note le chemin du fichier (typiquement `/etc/nginx/sites-available/feedback.noejoraz.org` ou similaire).

---

## 2. Ajouter les deux domaines à la config

Édite le fichier identifié à l'étape précédente :

```bash
sudo nano /etc/nginx/sites-available/<le-fichier-trouvé>
```

Dans **chaque** bloc `server { … }` (le bloc port 80 ET le bloc port 443), modifie la ligne `server_name` pour ajouter les deux nouveaux domaines.

**Avant :**

```nginx
server_name feedback.noejoraz.org;
```

**Après :**

```nginx
server_name feedback.noejoraz.org beta.scorenzer.com beta.serenzer.com;
```

Sauvegarde (`Ctrl+O`, `Enter`, `Ctrl+X`).

---

## 3. Tester et recharger Nginx

```bash
sudo nginx -t && sudo systemctl reload nginx
```

Si une erreur apparaît, **ne pas continuer** — me la transmettre.

---

## 4. Générer les certificats Let's Encrypt

```bash
sudo certbot --nginx \
  -d beta.scorenzer.com \
  -d beta.serenzer.com \
  --email <ton-email> \
  --agree-tos \
  --no-eff-email \
  --redirect
```

Remplace `<ton-email>` par ton adresse mail (utilisée pour les notifications d'expiration Let's Encrypt).

L'option `--nginx` détecte automatiquement la config existante, injecte les blocs SSL, et active la redirection HTTP → HTTPS.

---

## 5. Vérifier que tout répond bien en HTTPS

```bash
curl -I https://beta.scorenzer.com
curl -I https://beta.serenzer.com
```

Les deux doivent retourner un code 2xx, 3xx ou 4xx (peu importe — l'essentiel est qu'il n'y ait **pas** d'erreur SSL et que la réponse parle bien HTTP/2 ou HTTP/1.1).

---

## 6. Vérifier le renouvellement automatique

```bash
sudo certbot renew --dry-run
```

Le résultat doit contenir :

> *"Congratulations, all simulated renewals succeeded"*

Si oui, le renouvellement automatique fonctionne — aucune action manuelle requise dans 90 jours.

---

## À retourner après l'opération

- Sortie de `curl -I https://beta.scorenzer.com`
- Sortie de `curl -I https://beta.serenzer.com`
- Sortie de `sudo certbot renew --dry-run`

---

## Notes utiles

- **Firewall** : si UFW est actif, vérifier que les ports 80 et 443 sont ouverts (`sudo ufw status`). Sans ça, le challenge HTTP-01 échouera.
- **Wildcard** : on n'utilise pas de certificat wildcard ici, chaque sous-domaine est explicitement listé.
- **Conflit de configs** : si jamais une config par défaut (`/etc/nginx/sites-enabled/default`) capture `beta.*`, désactiver avec `sudo rm /etc/nginx/sites-enabled/default && sudo systemctl reload nginx`.
