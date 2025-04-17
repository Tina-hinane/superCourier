# Projet de Transformation des Données de Livraison

Ce projet vise à transformer et analyser les données de livraison en combinant plusieurs sources, en calculant des métriques importantes et en déterminant si une livraison est en retard ou à l'heure.

## Étapes du Processus

### 1. Joindre les données
Les données de livraison sont combinées avec d'autres sources, comme les données météorologiques, pour enrichir les informations disponibles.

### 2. Calculer les durées de livraison
Ajout d'une colonne calculant la durée réelle de livraison à partir des données brutes.

### 3. Enrichir avec les informations météorologiques
Utilisation de la fonction `enrich_with_weather` pour ajouter une colonne contenant les conditions météorologiques associées à chaque livraison.

### 4. Calculer les seuils de retard
La fonction `calculate_delay_threshold` est utilisée pour calculer un seuil de retard basé sur plusieurs facteurs, comme la distance, le type de colis, la zone de livraison, les conditions météorologiques, etc.

### 5. Déterminer si une livraison est en retard
Une colonne `is_delayed` est ajoutée pour indiquer si la durée réelle de livraison dépasse le seuil de retard calculé.

### 6. Ajouter le statut de la livraison
Une colonne `status` est ajoutée pour indiquer si une livraison est "On-time" ou "Delayed".

## Exemple d'Utilisation

### Script SQL
Pour visualiser les données transformées, utilisez la commande suivante dans un fichier SQL :
```sql
SELECT * FROM deliveries;
