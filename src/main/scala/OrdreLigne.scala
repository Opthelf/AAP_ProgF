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

  lazy val rerE_Ouest = Seq("Nanterre-La-Folie", "La Défense", "Neuilly - Porte Maillot", "Haussmann - Saint-Lazare", "Magenta", "Rosa Parks")
  lazy val rerE_Est_Chelles = Seq("Rosa Parks", "Pantin", "Noisy-le-Sec", "Bondy", "Le Raincy - Villemomble - Montfermeil", "Gagny", "Le Chénay - Gagny", "Chelles - Gournay")
  lazy val rerE_Est_Tournan = Seq("Rosa Parks", "Pantin", "Noisy-le-Sec", "Rosny-Bois-Perrier", "Rosny-sous-Bois", "Val de Fontenay", "Nogent - Le Perreux", "Les Boullereaux - Champigny", "Villiers-sur-Marne - Le Plessis-Trévise", "Les Yvris - Noisy-le-Grand", "Emerainville - Pontault-Combault", "Roissy-en-Brie", "Ozoir-la-Ferrière", "Gretz-Armainvilliers", "Tournan")
  lazy val rer_E = Seq(rerE_Ouest, rerE_Est_Chelles, rerE_Est_Tournan)

  val rerC_Troncon_Central = Seq(
    "Champ de Mars Tour Eiffel",
    "Pont de l'Alma",
    "Invalides",
    "Musée d'Orsay",
    "Saint-Michel Notre-Dame",
    "Gare d'Austerlitz"
  )

  // --- BRANCHE NORD (C1 / C3) ---
  // Elle se divise en deux après Ermont-Eaubonne ou Montigny selon les missions,
  // mais physiquement la ligne part de Champ de Mars vers le Nord.

  val rerC_Nord_Principal = Seq(
    "Champ de Mars Tour Eiffel",
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
    "Ermont - Eaubonne"
  )

  val rerC_Nord_Pontoise = Seq(
    "Ermont - Eaubonne",
    "Cernay",
    "Franconville - Le Plessis-Bouchard",
    "Montigny - Beauchamp",
    "Pierrelaye",
    "Saint-Ouen-l'Aumône - Liesse",
    "Saint-Ouen-l'Aumône",
    "Pontoise"
  )

  // --- BRANCHE OUEST (C5 / C7) ---
  // Elle part de Champ de Mars vers Versailles Rive Gauche / St Quentin

  val rerC_Ouest_Commun = Seq(
    "Champ de Mars Tour Eiffel",
    "Javel",
    "Pont du Garigliano",
    "Issy Val de Seine",
    "Issy",
    "Meudon Val Fleury",
    "Chaville - Vélizy",
    "Viroflay Rive Gauche"
  )

  // La fourche à Viroflay Rive Gauche
  val rerC_Ouest_Chateau = Seq(
    "Viroflay Rive Gauche",
    "Porchefontaine",
    "Versailles Château Rive Gauche"
  )

  val rerC_Ouest_StQuentin = Seq(
    "Viroflay Rive Gauche",
    "Versailles Chantiers",
    "Saint-Cyr",
    "Saint-Quentin-en-Yvelines"
  )

  // --- BRANCHE SUD (C2 / C4 / C6 / C8) ---
  // Elle part d'Austerlitz et descend vers le Sud.

  val rerC_Sud_Commun = Seq(
    "Gare d'Austerlitz",
    "Bibliothèque François Mitterrand",
    "Ivry-sur-Seine",
    "Vitry-sur-Seine",
    "Les Ardoines",
    "Choisy-le-Roi"
  )

  // Division à Choisy-le-Roi : Soit tout droit vers Juvisy, soit via Orly (Pont de Rungis)

  val rerC_Sud_Direct = Seq(
    "Choisy-le-Roi",
    "Villeneuve-le-Roi",
    "Ablon",
    "Athis-Mons",
    "Juvisy"
  )

  val rerC_Sud_Orly = Seq(
    "Choisy-le-Roi",
    "Les Saules",
    "Orly Ville",
    "Pont de Rungis - Aéroport d'Orly",
    "Rungis - La Fraternelle",
    "Chemin d'Antony",
    "Massy - Verrières",
    "Massy - Palaiseau"
  )

  // --- LA GRANDE BOUCLE SUD ---
  // De Juvisy, ça part vers Versailles Chantiers (via Massy) ou Dourdan/Etampes

  val rerC_Sud_Juvisy_Versailles = Seq(
    "Juvisy",
    "Savigny-sur-Orge",
    "Epinay-sur-Orge",
    "Sainte-Geneviève-des-Bois",
    "Saint-Michel-sur-Orge",
    "Brétigny" // Nœud important
  )

  // Jonction Massy (venant d'Orly) -> Versailles Chantiers
//  val rerC_Massy_Versailles = Seq(
//    "Massy - Palaiseau",
//    "Igny",
//    "Bièvres",
//    "Vauboyen",
//    "Jouy-en-Josas",
//    "Petit Jouy - Les Loges",
//    "Versailles Chantiers" // Rejoint la branche Ouest St-Quentin !
//  )

  // Jonction entre la ligne directe (Juvisy) et la ligne Orly à Massy
  // Note : Il y a des trains directs Juvisy -> Massy sans passer par Orly,
  // mais pour simplifier le graphe, on considère souvent la connexion à Juvisy ou Savigny.
  // Ajoutons la liaison Savigny -> Epinay (déjà dans Sud_Juvisy_Versailles)

  // Branches Extrêmes Sud (Dourdan / Etampes)
  val rerC_Sud_Dourdan = Seq(
    "Brétigny",
    "La Norville - Saint-Germain-lès-Arpajon",
    "Arpajon",
    "Egly",
    "Breuillet - Bruyères-le-Châtel",
    "Breuillet - Village",
    "Saint-Chéron",
    "Sermaise",
    "Dourdan",
    "Dourdan - La Forêt"
  )

  val rerC_Sud_Etampes = Seq(
    "Brétigny",
    "Marolles-en-Hurepoix",
    "Bouray",
    "Lardy",
    "Chamarande",
    "Etrechy",
    "Etampes",
    "Saint-Martin d'Etampes"
  )

  // --- AGGREGATION RER C ---
  lazy val rer_C = Seq(rerC_Troncon_Central, rerC_Nord_Principal, rerC_Nord_Pontoise, rerC_Ouest_Commun, rerC_Ouest_Chateau, rerC_Ouest_StQuentin, rerC_Sud_Commun, rerC_Sud_Direct, rerC_Sud_Orly, rerC_Sud_Juvisy_Versailles, rerC_Sud_Dourdan, rerC_Sud_Etampes)
}