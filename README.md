🛠️ Configuration et Prérequis
Ce projet est conçu pour fonctionner avec une stack technique spécifique. Pour garantir la compatibilité et éviter les erreurs de build, veuillez vous assurer que votre environnement est configuré comme suit.


//1. Prérequis Système (À installer)
Avant de cloner le projet, vous devez avoir les logiciels suivants installés et configurés sur votre système :

    Java Development Kit (JDK) :
    
    Version requise : JDK 17
    
    Vérification : java --version doit afficher une version 17.x.x.
    
    Note : Spark 4.0.x ne fonctionnera pas avec des versions plus anciennes comme JDK 8 ou 11.
    
    sbt (Simple Build Tool) :
    
    Version requise : 1.11.7 (ou toute version 1.9.x et supérieure).
    
    Vérification : sbt --version


//
2. Stack Technique du Projet (Gérée par sbt)
Vous n'avez pas besoin d'installer manuellement les éléments suivants. Ils sont définis dans le fichier build.sbt et seront téléchargés et gérés automatiquement par sbt lors du premier build :

    Scala : 2.13.16
    
    Apache Spark : 4.0.1
//
3. Build et Lancement
Une fois les prérequis système installés :

    Clonez ce dépôt.
    
    Ouvrez un terminal à la racine du projet.
    
    Pour compiler le projet et télécharger les dépendances :
    
    Bash
    
    sbt compile
    Pour exécuter la suite de tests :
    
    Bash
    
    sbt test
    Pour lancer l'application principale :
    
    Bash
    
    sbt run
