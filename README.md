Chaque jour vous recevrez les données de température (prisent à midi) des différentes stations gérées par Météo-France. Votre but est de fournir chaque jour un rapport au format parquet combinant les données des températures fourni par Météo-France avec les données de population des départements. Le résultat peut être constitué d'un ou plusieurs fichiers parquet.
Le rapport doit contenir l'intégralité des départements français et des stations Météo-France.
Si deux stations sont dans le même département, les infos de population doivent être dupliquées.
Si un département ne contient pas de station météorologique, des valeurs Null doivent apparaître.
Si une station n'est pas en France, elle doit être ignorée.
On rattachera les stations météo à la commune la plus proche géographiquement en utilisant les latitude et longitude pour déterminer son département


| Nom Colonne |  type  | description                                                      |
|-------------|--------|------------------------------------------------------------------|
| jour        | date   | date du relevé de température format YYYY-MM-DD                  |
| departement | int    | numéro du département                                            |
| population  | long   | population du département                                        |
| temperature | int    | température du jour à midi en °C                                 |
| id_station  | string | identifiant de la station Météo-France situé dans le département |


`Communes.csv`

| Nom    | description                    |
|--------|--------------------------------|
| DEPCOM | code INSEE de la commune       |
| PTOT   | population total de la commune |


`code-insee-postaux-geoflar.csv permet`

| Nom        | description                                |
|------------|--------------------------------------------|
| CODE INSEE | code INSEE de la commune                  |
| Code Dept  | numéro du département                      |
| geom_x_y   | latitude,longitude du centre de la commune |


`postesSynop.txt`

| Nom       | description                            |
|-----------|----------------------------------------|
| ID        | identifiant unique de la station météo |
| Latitude  | latitude de la station météo           |
| Longitude | longitude de la station météo          |


`synop.2020120512.txt`

| Nom       | description                                   |
|-----------|-----------------------------------------------|
| t         | température en Kelvin                         |
| numer_sta | identifiant unique de la station météo        |
| date      | date et heure du relevé format YYYYMMDDHHMMSS |

`doc_parametres_synop_168.pdf` description des colonnes du fichier de relevé de température


Pour la Conversion K °C il est demandé de réaliser une UDF  (t°C = K - 273,15) (1 pts)

Toutes les lectures de fichier doivent passer par des "object" ou des "class" n'ayant que des fonctions statiques. Ils devront aussi stocker le nom des colonnes utilisé sous forme de constante. (2 pts)

Le code doit être conçu de manière à être facilement testable (2 pts)
Les transformations sur les dataframe doivent être testées (2 pts)


Il est attendu une application sous forme de package egg ou jar, qui permettent de configurer les chemins des différents fichiers sans recompilation. (3 pts)

Traitement des fichiers météo (1 pts)
Traitement des fichiers de population (1 pts)
Réconciliation population et température (3 pts)

Le code doit être rendu dans un repository github privé accessible par l'enseignant (scauglog sur github). Seul les commit réalisé avant la fin de l'épreuve seront comptabilisés.

Vous avez le droit d'utiliser toutes les ressources disponibles sur internet et vues en cours.
Vous avez le droit de demander au prof d'éclaircir l'énoncé.
Vous avez le droit de demander au prof de l'aide sur les fonctions du language scala ou python.
Vous pouvez réaliser le TP dans un notebook, la note maximale que vous pourrez alors atteindre sera de 10/15
Vous n'avez pas le droit de demander de l'aide à vos camarades.

Vous devez envoyer l'URL de votre repo github à rsagean@xebia.fr
