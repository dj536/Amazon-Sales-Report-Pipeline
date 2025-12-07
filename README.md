# 📊 Amazon Sales Report - End-to-End DLT Pipeline

Ce projet implémente un pipeline pipeline Lakehouse entièrement automatisé permettant d’ingérer, transformer, historiser et analyser des données de ventes Amazon, en appliquant les principes Bronze → Silver → Gold, l’orchestration moderne avec LakeFlow Declarative Pipelines, et la modélisation analytique Data Warehouse (dimensions + fact table).


## 🚀 Architecture du Projet

Le pipeline repose sur l'architecture Medallion utilisant Delta Live Tables (DLT) :
Couche Bronze : Ingestion brute via cloudFiles depuis un Volume Unity Catalog. 
Couche Silver : Nettoyage, enrichissement temporel et agrégation. 
Couche Gold : Modélisation en Schéma en Étoile avec gestion de l'historique (SCD Type 2) et Clés de Substitution.

## 🛠️ Stack Technique

Plateforme : Databricks (Azure/AWS) 

Moteur de Données : Delta Live Tables (DLT)

Langages : PySpark (ETL) & SQL (Reporting)

Gouvernance : Unity Catalog


📁 Structure des Données (Star Schema)

Le modèle dimensionnel final permet des analyses croisées sur 5 perspectives clés:

Fact_Sales : SalesKey, ProductKey, TimeKey, LocationKey, SalesChannelKey, OrderStatusKey, Qty

Dim_Product : SKU, Style, Category, Size, ProductCode, Line 

Dim_Time : Date, Day, Month, Quarter, Year, Week, TimeKey 

Dim_Location : ShipState, ShipPostalCode, ShipCountry, location_key 

Dim_Sales_Channel : fulfilmentType, servicelevel, channelKey 

Dim_Order_Status : orderStatus, StatusCategory, status_key
