package projet

object OrdreLigne {
  lazy val brancheA1 = Seq("Saint-Germain-en-Laye", "Le Vésinet - Le Pecq", "Le Vésinet-Centre", "Chatou-Croissy", "Rueil-Malmaison", "Nanterre-Ville", "Nanterre-Université", "Nanterre-Préfecture")
  lazy val brancheA3 = Seq("Cergy-Le Haut", "Cergy-Saint-Christophe", "Cergy-Préfecture", "Neuville-Université", "Conflans-Fin-d'Oise", "Achères-Ville", "Maisons-Laffitte", "Sartrouville", "Houilles-Carrières-sur-Seine", "Nanterre-Préfecture")
  lazy val brancheA5 = Seq("Poissy", "Achères-Grand-Cormier", "Maisons-Laffitte") // Rejoint la A3
  lazy val tronconCentral = Seq("Nanterre-Préfecture", "La Défense", "Charles de Gaulle - Etoile", "Auber", "Châtelet les Halles", "Gare de Lyon", "Nation", "Vincennes")
  lazy val brancheA2 = Seq("Vincennes", "Fontenay-sous-Bois", "Nogent-sur-Marne", "Joinville-le-Pont", "Saint-Maur - Créteil", "Le Parc de Saint-Maur", "Champigny", "La Varenne - Chennevières", "Sucy - Bonneuil", "Boissy-Saint-Léger")
  lazy val brancheA4 = Seq("Vincennes", "Val de Fontenay", "Neuilly-Plaisance", "Bry-sur-Marne", "Noisy-le-Grand - Mont d'Est", "Noisy - Champs", "Noisiel", "Lognes", "Torcy", "Bussy-Saint-Georges", "Val d'Europe", "Marne-la-Vallée - Chessy")
  lazy val rer_A = Seq(brancheA1, brancheA3, brancheA5, tronconCentral, brancheA2, brancheA4)

  lazy val rerB_Nord_CDG = Seq("Aéroport Charles de Gaulle 2 TGV", "Aéroport Charles de Gaulle 1", "Parc des Expositions", "Villepinte", "Sevran - Beaudottes", "Aulnay-sous-Bois")
  lazy val rerB_Nord_Mitry = Seq("Mitry - Claye", "Villeparisis - Mitry-le-Neuf", "Vert-Galant", "Sevran - Livry", "Aulnay-sous-Bois")
  lazy val rerB_Centre = Seq("Aulnay-sous-Bois", "Le Blanc-Mesnil", "Drancy", "Le Bourget", "La Courneuve - Aubervilliers", "La Plaine - Stade de France", "Gare du Nord", "Châtelet les Halles", "Saint-Michel - Notre-Dame", "Luxembourg", "Port-Royal", "Denfert-Rochereau", "Cité Universitaire", "Gentilly", "Laplace", "Arcueil - Cachan", "Bagneux", "Bourg-la-Reine")
  lazy val rerB_Sud_Robinson = Seq("Bourg-la-Reine", "Sceaux", "Fontenay-aux-Roses", "Robinson")
  lazy val rerB_Sud_StRemy = Seq("Bourg-la-Reine", "Parc de Sceaux", "La Croix de Berny", "Antony", "Fontaine Michalon", "Les Baconnets", "Massy - Verrières", "Massy - Palaiseau", "Palaiseau", "Palaiseau - Villebon", "Lozère", "Le Guichet", "Orsay - Ville", "Bures-sur-Yvette", "La Hacquinière", "Gif-sur-Yvette", "Courcelle-sur-Yvette", "Saint-Rémy-lès-Chevreuse")
  lazy val rer_B = Seq(rerB_Nord_CDG, rerB_Nord_Mitry, rerB_Centre, rerB_Sud_Robinson, rerB_Sud_StRemy)

  lazy val rerD_Nord_Creil = Seq("Creil", "Chantilly - Gouvieux", "Orry-la-Ville - Coye", "La Borne Blanche", "Survilliers - Fosses", "Louvres", "Les Noues", "Goussainville", "Villiers-le-Bel - Gonesse - Arnouville", "Garges - Sarcelles", "Pierrefitte - Stains", "Saint-Denis", "Gare du Nord")
  lazy val rerD_Centre = Seq("Gare du Nord", "Châtelet les Halles", "Gare de Lyon", "Maisons-Alfort - Alfortville", "Le Vert de Maisons", "Créteil - Pompadour", "Villeneuve - Triage", "Villeneuve-Saint-Georges")
  lazy val rerD_Sud_Melun_Combs = Seq("Villeneuve-Saint-Georges", "Montgeron - Crosne", "Yerres", "Brunoy", "Boussy-Saint-Antoine", "Combs-la-Ville - Quincy", "Lieusaint - Moissy", "Savigny-le-Temple - Nandy", "Cesson", "Le Mée", "Melun")
  lazy val rerD_Sud_Plateau = Seq("Villeneuve-Saint-Georges", "Vigneux-sur-Seine", "Juvisy", "Viry-Châtillon", "Ris-Orangis", "Grand Bourg", "Evry - Val de Seine", "Corbeil-Essonnes")
  lazy val rerD_Sud_Malsherbes = Seq("Corbeil-Essonnes", "Mennecy", "Ballancourt", "La Ferté-Alais", "Boutigny", "Maisse", "Buno - Gironville", "Boigneville", "Malesherbes")
  lazy val rerD_Sud_Melun_Corbeil = Seq("Corbeil-Essonnes", "Essonnes - Robinson", "Villabé", "Le Plessis-Chenet", "Coudray-Montceaux", "Saint-Fargeau", "Ponthierry - Pringy", "Boissise-le-Roi", "Vosves", "Melun")
  lazy val rer_D = Seq(rerD_Nord_Creil, rerD_Centre, rerD_Sud_Melun_Combs, rerD_Sud_Plateau, rerD_Sud_Malsherbes, rerD_Sud_Melun_Corbeil)

  lazy val rerE_Ouest = Seq("Nanterre-La-Folie",
    "La Défense",
    "Neuilly - Porte Maillot",
    "Haussmann - Saint-Lazare",
    "Magenta",
    "Rosa Parks")
  lazy val rerE_Est_Chelles = Seq("Rosa Parks",
    "Pantin",
    "Noisy-le-Sec",
    "Bondy",
    "Le Raincy - Villemomble - Montfermeil",
    "Gagny",
    "Le Chénay - Gagny",
    "Chelles - Gournay")
  lazy val rerE_Est_Tournan = Seq("Rosa Parks",
    "Pantin",
    "Noisy-le-Sec",
    "Rosny-Bois-Perrier",
    "Rosny-sous-Bois",
    "Val de Fontenay",
    "Nogent - Le Perreux",
    "Les Boullereaux - Champigny",
    "Villiers-sur-Marne - Le Plessis-Trévise",
    "Les Yvris - Noisy-le-Grand",
    "Emerainville - Pontault-Combault",
    "Roissy-en-Brie",
    "Ozoir-la-Ferrière",
    "Gretz-Armainvilliers",
    "Tournan")
  lazy val rer_E = Seq(rerE_Ouest, rerE_Est_Chelles, rerE_Est_Tournan)

  val rerC_Troncon_Central = Seq("Champ de Mars Tour Eiffel",
    "Pont de l'Alma",
    "Invalides",
    "Musée d'Orsay",
    "Saint-Michel Notre-Dame",
    "Gare d'Austerlitz")
  // --- BRANCHE NORD (C1 / C3) ---
  // Elle se divise en deux après Ermont-Eaubonne ou Montigny selon les missions,
  // mais physiquement la ligne part de Champ de Mars vers le Nord.

  lazy val rerC_Nord_Principal = Seq("Champ de Mars Tour Eiffel",
    "Avenue du Président Kennedy",
    "Boulainvilliers",
    "Avenue Henri Martin",
    "Avenue Foch",
    "Neuilly - Porte Maillot",
    "Pereire - Levallois",
    "Porte de Clichy",
    "Saint-Ouen",
    "Les Grésillons",
    "Gennevilliers",
    "Epinay-sur-Seine",
    "Saint-Gratien",
    "Ermont - Eaubonne")
  lazy val rerC_Nord_Pontoise = Seq("Ermont - Eaubonne",
    "Cernay",
    "Franconville - Le Plessis-Bouchard",
    "Montigny - Beauchamp",
    "Pierrelaye",
    "Saint-Ouen-l'Aumône - Liesse",
    "Saint-Ouen-l'Aumône",
    "Pontoise")
  // --- BRANCHE OUEST (C5 / C7) ---
  // Elle part de Champ de Mars vers Versailles Rive Gauche / St Quentin
  lazy val rerC_Ouest_Commun = Seq("Champ de Mars Tour Eiffel",
    "Javel",
    "Pont du Garigliano",
    "Issy Val de Seine",
    "Issy",
    "Meudon Val Fleury",
    "Chaville - Vélizy",
    "Viroflay Rive Gauche")
  // La fourche à Viroflay Rive Gauche
  lazy val rerC_Ouest_Chateau = Seq("Viroflay Rive Gauche",
    "Porchefontaine",
    "Versailles Château Rive Gauche")
  lazy val rerC_Ouest_StQuentin = Seq("Viroflay Rive Gauche",
    "Versailles Chantiers",
    "Saint-Cyr",
    "Saint-Quentin-en-Yvelines")
  // --- BRANCHE SUD (C2 / C4 / C6 / C8) ---
  // Elle part d'Austerlitz et descend vers le Sud.
  lazy val rerC_Sud_Commun = Seq("Gare d'Austerlitz",
    "Bibliothèque François Mitterrand",
    "Ivry-sur-Seine",
    "Vitry-sur-Seine",
    "Les Ardoines",
    "Choisy-le-Roi")
  // Division à Choisy-le-Roi : Soit tout droit vers Juvisy, soit via Orly (Pont de Rungis)
  lazy val rerC_Sud_Direct = Seq("Choisy-le-Roi",
    "Villeneuve-le-Roi",
    "Ablon",
    "Athis-Mons",
    "Juvisy")
  lazy val rerC_Sud_Orly = Seq("Choisy-le-Roi",
    "Les Saules",
    "Orly Ville",
    "Pont de Rungis - Aéroport d'Orly",
    "Rungis - La Fraternelle",
    "Chemin d'Antony",
    "Massy - Verrières",
    "Massy - Palaiseau")
  // --- LA GRANDE BOUCLE SUD ---
  // De Juvisy, ça part vers Versailles Chantiers (via Massy) ou Dourdan/Etampes
  val rerC_Sud_Juvisy_Versailles = Seq("Juvisy",
    "Savigny-sur-Orge",
    "Epinay-sur-Orge",
    "Sainte-Geneviève-des-Bois",
    "Saint-Michel-sur-Orge",
    "Brétigny") // Nœud important)
  // Branches Extrêmes Sud (Dourdan / Etampes)
  lazy val rerC_Sud_Dourdan = Seq("Brétigny",
    "La Norville - Saint-Germain-lès-Arpajon",
    "Arpajon",
    "Egly",
    "Breuillet - Bruyères-le-Châtel",
    "Breuillet - Village",
    "Saint-Chéron",
    "Sermaise",
    "Dourdan",
    "Dourdan - La Forêt")
  lazy val rerC_Sud_Etampes = Seq("Brétigny",
    "Marolles-en-Hurepoix",
    "Bouray",
    "Lardy",
    "Chamarande",
    "Etrechy",
    "Etampes",
    "Saint-Martin d'Etampes")
  // --- AGGREGATION RER C ---
  lazy val rer_C = Seq(rerC_Troncon_Central,
    rerC_Nord_Principal,
    rerC_Nord_Pontoise,
    rerC_Ouest_Commun,
    rerC_Ouest_Chateau,
    rerC_Ouest_StQuentin,
    rerC_Sud_Commun,
    rerC_Sud_Direct,
    rerC_Sud_Orly,
    rerC_Sud_Juvisy_Versailles,
    rerC_Sud_Dourdan,
    rerC_Sud_Etampes)


  //Métro 1
  lazy val metro_Ligne1 = Seq(
    "La Défense", // Connexion RER A
    "Esplanade de la Défense",
    "Pont de Neuilly",
    "Les Sablons",
    "Porte Maillot",
    "Argentine",
    "Charles de Gaulle - Etoile", // Connexion RER A
    "George V",
    "Franklin D. Roosevelt", // VOTRE STATION VIP
    "Champs-Elysées - Clemenceau",
    "Concorde",
    "Tuileries",
    "Palais Royal - Musée du Louvre",
    "Louvre - Rivoli",
    "Châtelet les Halles", // Connexion RER A (Simplification pour Châtelet)
    "Hôtel de Ville",
    "Saint-Paul",
    "Bastille",
    "Gare de Lyon", // Connexion RER A
    "Reuilly - Diderot",
    "Nation", // Connexion RER A
    "Porte de Vincennes",
    "Saint-Mandé",
    "Bérault",
    "Château de Vincennes"
  )
  lazy val metro_Ligne2 = Seq(
    "Porte Dauphine",
    "Victor Hugo",
    "Charles de Gaulle - Etoile", // Hub RER A
    "Ternes",
    "Courcelles",
    "Monceau",
    "Villiers",
    "Rome",
    "Place de Clichy",
    "Blanche",
    "Pigalle",
    "Anvers",
    "Barbès - Rochechouart",
    "La Chapelle",
    "Stalingrad",
    "Jaurès",
    "Colonel Fabien",
    "Belleville",
    "Couronnes",
    "Ménilmontant",
    "Père Lachaise",
    "Philippe Auguste",
    "Alexandre Dumas",
    "Avron",
    "Nation"                      // Hub RER A
  )
  lazy val metro_Ligne3 = Seq(
    "Pont de Levallois - Bécon",
    "Anatole France",
    "Louise Michel",
    "Porte de Champerret",
    "Pereire",
    "Wagram",
    "Malesherbes",
    "Villiers",
    "Europe",
    "Saint-Lazare",               // Hub RER E
    "Havre - Caumartin",          // Connecté Auber
    "Opéra",
    "Quatre-Septembre",
    "Bourse",
    "Sentier",
    "Réaumur - Sébastopol",
    "Arts et Métiers",
    "Temple",
    "République",
    "Parmentier",
    "Rue Saint-Maur",
    "Père Lachaise",
    "Gambetta",
    "Porte de Bagnolet",
    "Gallieni"
  )
  lazy val metro_Ligne3bis = Seq(
    "Gambetta",
    "Pelleport",
    "Saint-Fargeau",
    "Porte des Lilas"
  )
  lazy val metro_Ligne4 = Seq(
    "Porte de Clignancourt",
    "Simplon",
    "Marcadet - Poissonniers",
    "Château Rouge",
    "Barbès - Rochechouart",
    "Gare du Nord", // Connexion RER B / D
    "Gare de l'Est",
    "Château d'Eau",
    "Strasbourg - Saint-Denis",
    "Réaumur - Sébastopol",
    "Étienne Marcel",
    "Les Halles",
    "Châtelet les Halles", // Connexion RER A (Fusion Châtelet + Les Halles)
    "Cité",
    "Saint-Michel", // Connexion RER B / C (Souvent St-Michel Notre-Dame)
    "Odéon",
    "Saint-Germain-des-Prés", // VOTRE STATION VIP
    "Saint-Sulpice",
    "Saint-Placide",
    "Montparnasse - Bienvenüe",
    "Vavin",
    "Raspail",
    "Denfert-Rochereau", // Connexion RER B
    "Mouton-Duvernet",
    "Alésia",
    "Porte d'Orléans",
    "Mairie de Montrouge",
    "Barbara",
    "Bagneux - Lucie Aubrac"
  )
  lazy val metro_Ligne5 = Seq(
    "Bobigny - Pablo Picasso",
    "Bobigny - Pantin - Raymond Queneau",
    "Église de Pantin",
    "Hoche",
    "Porte de Pantin",
    "Ourcq",
    "Laumière",
    "Jaurès",
    "Stalingrad",
    "Gare du Nord",               // Hub RER B/D/E
    "Gare de l'Est",
    "Jacques Bonsergent",
    "République",
    "Oberkampf",
    "Richard-Lenoir",
    "Bréguet - Sabin",
    "Bastille",
    "Quai de la Rapée",
    "Gare d'Austerlitz",          // Hub RER C
    "Saint-Marcel",
    "Campo-Formio",
    "Place d'Italie"
  )
  lazy val metro_Ligne6 = Seq(
    "Charles de Gaulle - Etoile", // Hub RER A
    "Kléber",
    "Boissière",
    "Trocadéro",
    "Passy",
    "Bir-Hakeim",
    "Dupleix",
    "La Motte-Picquet - Grenelle",
    "Cambronne",
    "Sèvres - Lecourbe",
    "Pasteur",
    "Montparnasse - Bienvenüe",
    "Edgar Quinet",
    "Raspail",
    "Denfert-Rochereau",          // Hub RER B
    "Saint-Jacques",
    "Glacière",
    "Corvisart",
    "Place d'Italie",
    "Nationale",
    "Chevaleret",
    "Quai de la Gare",
    "Bercy",
    "Dugommier",
    "Daumesnil",
    "Bel-Air",
    "Picpus",
    "Nation"                      // Hub RER A
  )
  lazy val m7_Troncon = Seq(
    "La Courneuve - 8 Mai 1945",
    "Fort d'Aubervilliers",
    "Aubervilliers - Pantin - Quatre Chemins",
    "Porte de la Villette",
    "Corentin Cariou",
    "Crimée",
    "Riquet",
    "Stalingrad",
    "Louis Blanc",
    "Château-Landon",
    "Gare de l'Est",
    "Poissonnière",
    "Cadet",
    "Le Peletier",
    "Chaussée d'Antin - La Fayette",
    "Opéra",
    "Pyramides",
    "Palais Royal - Musée du Louvre",
    "Pont Neuf",
    "Châtelet les Halles",        // Hub RER A/B/D
    "Pont Marie",
    "Sully - Morland",
    "Jussieu",
    "Place Monge",
    "Censier - Daubenton",
    "Les Gobelins",
    "Place d'Italie",
    "Tolbiac",
    "Maison Blanche"
  )
  lazy val m7_Branche_Ivry = Seq(
    "Maison Blanche",
    "Porte d'Italie",
    "Porte de Choisy",
    "Porte d'Ivry",
    "Pierre et Marie Curie",
    "Mairie d'Ivry"
  )
  lazy val m7_Branche_Villejuif = Seq(
    "Maison Blanche",
    "Le Kremlin-Bicêtre",
    "Villejuif - Léo Lagrange",
    "Villejuif - Paul Vaillant-Couturier",
    "Villejuif - Louis Aragon"
  )
  lazy val metro_Ligne7 = Seq(m7_Troncon,m7_Branche_Ivry,m7_Branche_Villejuif)
  lazy val metro_Ligne7bis = Seq(
    "Louis Blanc",       // Connexion Ligne 7
    "Jaurès",            // Connexion Ligne 2 / 5
    "Bolivar",
    "Buttes Chaumont",
    "Botzaris",
    "Place des Fêtes",   // Connexion Ligne 11
    "Pré-Saint-Gervais",
    "Danube",
    "Botzaris"           // Retour à la bifurcation pour fermer la boucle dans le graphe
  )
  lazy val metro_Ligne8 = Seq(
    "Balard",
    "Lourmel",
    "Boucicaut",
    "Félix Faure",
    "Commerce",
    "La Motte-Picquet - Grenelle",
    "École Militaire",
    "La Tour-Maubourg",
    "Invalides",                  // Hub RER C
    "Concorde",
    "Madeleine",
    "Opéra",
    "Richelieu - Drouot",
    "Grands Boulevards",
    "Bonne Nouvelle",
    "Strasbourg - Saint-Denis",
    "République",
    "Filles du Calvaire",
    "Saint-Sébastien - Froissart",
    "Chemin Vert",
    "Bastille",
    "Ledru-Rollin",
    "Faidherbe - Chaligny",
    "Reuilly - Diderot",
    "Montgallet",
    "Daumesnil",
    "Michel Bizot",
    "Porte Dorée",
    "Porte de Charenton",
    "Liberté",
    "Charenton - Écoles",
    "École Vétérinaire de Maisons-Alfort",
    "Maisons-Alfort - Stade",
    "Maisons-Alfort - Les Juilliottes",
    "Créteil - L'Échat",
    "Créteil - Université",
    "Créteil - Préfecture",
    "Pointe du Lac"
  )
  lazy val metro_Ligne9 = Seq(
    "Pont de Sèvres",
    "Billancourt",
    "Marcel Sembat",
    "Porte de Saint-Cloud",
    "Exelmans",
    "Michel-Ange - Molitor",
    "Michel-Ange - Auteuil",
    "Jasmin",
    "Ranelagh",
    "La Muette",
    "Rue de la Pompe",
    "Trocadéro",
    "Iéna",
    "Alma - Marceau",
    "Franklin D. Roosevelt",      // Station VIP
    "Saint-Philippe du Roule",
    "Miromesnil",
    "Saint-Augustin",
    "Havre - Caumartin",          // Connecté Auber
    "Chaussée d'Antin - La Fayette",
    "Richelieu - Drouot",
    "Grands Boulevards",
    "Bonne Nouvelle",
    "Strasbourg - Saint-Denis",
    "République",
    "Oberkampf",
    "Saint-Ambroise",
    "Voltaire",
    "Charonne",
    "Rue des Boulets",
    "Nation",                     // Hub RER A
    "Buzenval",
    "Maraîchers",
    "Porte de Montreuil",
    "Robespierre",
    "Croix de Chavaux",
    "Mairie de Montreuil"
  )

  lazy val m10_Est = Seq(
    "Gare d'Austerlitz",          // Hub RER C / Ligne 5
    "Jussieu",                    // Hub Ligne 7
    "Cardinal Lemoine",
    "Maubert - Mutualité",
    "Cluny - La Sorbonne",        // Connexion St-Michel (RER B/C)
    "Odéon",                      // Hub Ligne 4
    "Mabillon",
    "Sèvres - Babylone",          // Hub Ligne 12
    "Vaneau",
    "Duroc",                      // Hub Ligne 13
    "Ségur",
    "La Motte-Picquet - Grenelle",// Hub Ligne 6 / 8
    "Avenue Émile Zola",
    "Charles Michels",
    "Javel - André Citroën"       // PIVOT EST de la boucle
  )
  lazy val m10_Boucle_Nord = Seq(
    "Javel - André Citroën",      // Départ Pivot
    "Église d'Auteuil",
    "Michel-Ange - Auteuil",      // Hub Ligne 9
    "Porte d'Auteuil",
    "Boulogne - Jean Jaurès"      // Arrivée PIVOT OUEST
  )
  lazy val m10_Boucle_Sud = Seq(
    "Boulogne - Jean Jaurès",     // Départ Pivot
    "Michel-Ange - Molitor",      // Hub Ligne 9
    "Chardon-Lagache",
    "Mirabeau",
    "Javel - André Citroën"       // Arrivée Pivot
  )
  lazy val m10_Terminus = Seq(
    "Boulogne - Jean Jaurès",
    "Boulogne - Pont de Saint-Cloud"
  )
  lazy val metro_Ligne10 = Seq(m10_Est, m10_Boucle_Nord, m10_Boucle_Sud, m10_Terminus)

  lazy val metro_Ligne11 = Seq(
    "Châtelet les Halles",        // Hub RER A/B/D
    "Hôtel de Ville",
    "Rambuteau",
    "Arts et Métiers",
    "République",
    "Goncourt",
    "Belleville",
    "Pyrénées",
    "Jourdain",
    "Place des Fêtes",
    "Télégraphe",
    "Porte des Lilas",
    "Mairie des Lilas",
    "Serge Gainsbourg",
    "Romainville - Carnot",
    "Montreuil - Hôpital",
    "La Dhuys",
    "Coteaux Beauclair",
    "Rosny-Bois-Perrier"          // Hub RER E
  )
  lazy val metro_Ligne12 = Seq(
    "Mairie d'Aubervilliers",
    "Aimé Césaire",
    "Front Populaire",
    "Porte de la Chapelle",
    "Marx Dormoy",
    "Marcadet - Poissonniers",
    "Jules Joffrin",
    "Lamarck - Caulaincourt",
    "Abbesses",
    "Pigalle",
    "Saint-Georges",
    "Notre-Dame-de-Lorette",
    "Trinité - d'Estienne d'Orves",
    "Saint-Lazare",               // Hub RER E
    "Madeleine",
    "Concorde",
    "Assemblée Nationale",
    "Solférino",
    "Rue du Bac",
    "Sèvres - Babylone",
    "Rennes",
    "Notre-Dame-des-Champs",
    "Montparnasse - Bienvenüe",
    "Falguière",
    "Pasteur",
    "Volontaires",
    "Vaugirard",
    "Convention",
    "Porte de Versailles",
    "Corentin Celton",
    "Mairie d'Issy"
  )

  lazy val m13_Branche_Asnieres = Seq(
    "Les Courtilles",
    "Les Agnettes",
    "Gabriel Péri",
    "Mairie de Clichy",
    "Porte de Clichy",            // Hub RER C
    "Brochant",
    "La Fourche"
  )
  lazy val m13_Branche_StDenis = Seq(
    "Saint-Denis - Université",
    "Basilique de Saint-Denis",
    "Saint-Denis - Porte de Paris",
    "Carrefour Pleyel",
    "Mairie de Saint-Ouen",
    "Garibaldi",
    "Porte de Saint-Ouen",
    "Guy Môquet",
    "La Fourche"
  )
  lazy val m13_Troncon = Seq(
    "La Fourche",
    "Place de Clichy",
    "Liège",
    "Saint-Lazare",               // Hub RER E
    "Miromesnil",
    "Champs-Elysées - Clemenceau",
    "Invalides",                  // Hub RER C
    "Varenne",
    "Saint-François-Xavier",
    "Duroc",
    "Montparnasse - Bienvenüe",
    "Gaîté",
    "Pernety",
    "Plaisance",
    "Porte de Vanves",
    "Malakoff - Plateau de Vanves",
    "Malakoff - Rue Étienne Dolet",
    "Châtillon - Montrouge"
  )
  lazy val metro_Ligne13 = Seq(m13_Troncon, m13_Branche_Asnieres, m13_Branche_StDenis)

  lazy val metro_Ligne14 = Seq(
    "Saint-Denis Pleyel",
    "Mairie de Saint-Ouen",
    "Porte de Clichy",
    "Pont Cardinet",
    "Saint-Lazare",               // Hub RER E
    "Madeleine",
    "Pyramides",
    "Châtelet les Halles",        // Hub RER A/B/D
    "Gare de Lyon",               // Hub RER A/D
    "Bercy",
    "Cour Saint-Émilion",
    "Bibliothèque François Mitterrand", // Hub RER C
    "Olympiades",
    "Maison Blanche",
    "Hôpital Bicêtre",
    "Villejuif - Gustave Roussy",
    "L'Haÿ-les-Roses",
    "Chevilly-Larue",
    "Thiais - Orly",
    "Aéroport d'Orly"             // Hub RER C (Pt Rungis) / B (Antony/Orlyval)
  )

  lazy val definitionReseauMetro = Seq(
    (metro_Ligne1,"Métro 1"),
    (metro_Ligne2,"Métro 2"),
    (metro_Ligne3,"Métro 3"),
    (metro_Ligne3bis,"Métro 3B"),
    (metro_Ligne4,"Métro 4"),
    (metro_Ligne5,"Métro 5"),
    (metro_Ligne6,"Métro 6"),

    (m7_Branche_Ivry,"Métro 7"),
    (m7_Troncon,"Métro 7"),
    (m7_Branche_Villejuif,"Métro 7"),

    (metro_Ligne7bis,"Métro 7B"),
    (metro_Ligne8,"Métro 8"),
    (metro_Ligne9,"Métro 9"),

    (m10_Boucle_Nord,"Métro 10"),
    (m10_Boucle_Sud,"Métro 10"),
    (m10_Terminus,"Métro 10"),
    (m10_Est,"Métro 10"),

    (metro_Ligne11,"Métro 11"),
    (metro_Ligne12,"Métro 12"),

    (m13_Branche_Asnieres,"Métro 13"),
    (m13_Troncon,"Métro 13"),
    (m13_Branche_StDenis,"Métro 13"),
    (metro_Ligne14,"Métro 14")


  )

  lazy val definitionReseauRER = Seq(
    (brancheA1, "RER A"),
    (brancheA3, "RER A"),
    (brancheA5, "RER A"),
    (tronconCentral, "RER A"),
    (brancheA2, "RER A"),
    (brancheA4, "RER A"),

    (rerB_Nord_CDG,"RER B"),
    (rerB_Nord_Mitry,"RER B"),
    (rerB_Centre,"RER B"),
    (rerB_Sud_Robinson,"RER B"),
    (rerB_Sud_StRemy,"RER B"),

    (rerD_Nord_Creil,"RER D"),
    (rerD_Centre, "RER D"),
    (rerD_Sud_Melun_Combs,"RER D"),
    (rerD_Sud_Plateau,"RER D"),
    (rerD_Sud_Malsherbes,"RER D"),
    (rerD_Sud_Melun_Corbeil,"RER D"),

    (rerE_Ouest,"RER E"),
    (rerE_Est_Chelles, "RER E"),
    (rerE_Est_Tournan,"RER E"),

    (rerC_Troncon_Central,"RER C"),
    (rerC_Nord_Principal,"RER C"),
    (rerC_Nord_Pontoise,"RER C"),
    (rerC_Ouest_Commun,"RER C"),
    (rerC_Ouest_Chateau,"RER C"),
    (rerC_Ouest_StQuentin,"RER C"),
    (rerC_Sud_Commun,"RER C"),
    (rerC_Sud_Direct,"RER C"),
    (rerC_Sud_Orly,"RER C"),
    (rerC_Sud_Juvisy_Versailles,"RER C"),
    (rerC_Sud_Dourdan,"RER C"),
    (rerC_Sud_Etampes,"RER C")


















  )

  lazy val definitionReseau = Seq(
    (brancheA1, "RER A"),
    (brancheA3, "RER A"),
    (brancheA5, "RER A"),
    (tronconCentral, "RER A"),
    (brancheA2, "RER A"),
    (brancheA4, "RER A"),
    (metro_Ligne1, "Métro 1"),
    (metro_Ligne4, "Métro 4")
  )
  lazy val tousLesTroncons = definitionReseau.map(_._1)

  lazy val tousLesTronconsMetro = definitionReseauMetro.map(_._1)
}