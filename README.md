# 📊 Amazon Sales Report - End-to-End DLT Pipeline

Ce projet implémente un pipeline pipeline Lakehouse entièrement automatisé permettant d’ingérer, transformer, historiser et analyser des données de ventes Amazon, en appliquant les principes Bronze → Silver → Gold, l’orchestration moderne avec LakeFlow Declarative Pipelines, et la modélisation analytique Data Warehouse (dimensions + fact table).


## 🚀 Architecture du Projet

Le pipeline repose sur l'architecture Medallion utilisant Delta Live Tables (DLT) :

Couche Bronze : Ingestion brute via cloudFiles depuis un Volume Unity Catalog; utilisation de l'Auto Loader de Databricks. Contrairement à une ingestion classique, Auto Loader détecte automatiquement les nouveaux fichiers CSV arrivant dans le Volume Unity Catalog et ne traite que les nouveautés (incrémental). On définit également les règles (rule1, rule2...) qui marquent les lignes suspectes

Couche Silver : Nettoyage, enrichissement temporel et agrégation, elle contient des données propres, filtrées , filtrées, transformées, aggrégées et prêtes pour le DWH.

Couche Gold : Modélisation en Schéma en Étoile avec gestion de l'historique (SCD Type 2) et Clés de Substitution, la couche finale transforme les flux de données en un Schéma en Étoile (Star Schema), pour l'aide à la décision.

## 🛠️ Stack Technique

Plateforme : Databricks (Azure/AWS) 

Moteur de Données : Delta Live Tables (DLT)

Langages : PySpark (ETL) & SQL (Reporting)

Gouvernance : Unity Catalog


## 📁 Structure des Données (Star Schema)

Le modèle dimensionnel final permet des analyses croisées sur 5 perspectives clés:

Fact_Sales : SalesKey, ProductKey, TimeKey, LocationKey, SalesChannelKey, OrderStatusKey, Qty

Dim_Product : SKU, Style, Category, Size, ProductCode, Line 

Dim_Time : Date, Day, Month, Quarter, Year, Week, TimeKey 

Dim_Location : ShipState, ShipPostalCode, ShipCountry, location_key 

Dim_Sales_Channel : fulfilmentType, servicelevel, channelKey 

Dim_Order_Status : orderStatus, StatusCategory, status_key


## 📂 Project Structure
```
├── explorations/
│   └── sample_explorations.py       
├── transformations/
│   ├── Bronze/
│   │   └── data_ingestion.py        
│   ├── Silver/
│   │   ├── amazon_strmg.py        
│   │   └── aggregation.py     
│   └── Gold/
│       ├── Fact_layer.py       
│       └──dim Order_status.py
│       └── dim location.py
│       └── dim product.py
│       └── dim time.py
│       └── dim_salesChannel.py        
├  
├── volumes/
│   └── data/                   
└── README.md                   
```


## 🛠️ Getting Started

Databricks Workspace: Un compte actif avec un cluster fonctionnel:
```
https://www.databricks.com/fr/learn/free-edition
```
