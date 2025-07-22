
        CREATE TABLE types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        curie TEXT NOT NULL UNIQUE);
    

        CREATE TABLE identifiers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        curie TEXT NOT NULL UNIQUE,
        label TEXT);
    

        CREATE TABLE cliques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        primary_identifier_id INTEGER NOT NULL,
        ic REAL,
        type_id INTEGER NOT NULL,
        preferred_name TEXT NOT NULL,
        FOREIGN KEY(primary_identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(type_id) REFERENCES types(id),
        UNIQUE(primary_identifier_id, type_id));
    

        CREATE TABLE descriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        desc TEXT NOT NULL);
    

        CREATE TABLE identifiers_descriptions (
        description_id INTEGER NOT NULL,
        identifier_id INTEGER NOT NULL,
        FOREIGN KEY(description_id) REFERENCES descriptions(id),
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id));
    

        CREATE TABLE identifiers_cliques (
        identifier_id INTEGER NOT NULL,
        clique_id INTEGER NOT NULL,
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(clique_id) REFERENCES cliques(id));
    

        CREATE TABLE identifiers_taxa (
        identifier_id INTEGER NOT NULL,
        taxa_identifier_id INTEGER NOT NULL,
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(taxa_identifier_id) REFERENCES identifiers(id));
    

        CREATE TABLE conflation_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type INTEGER NOT NULL CHECK (type in
        (1, 2)));
    

        CREATE TABLE conflation_members (
        cluster_id INTEGER NOT NULL,
        curie_id INTEGER NOT NULL,
        FOREIGN KEY(cluster_id) REFERENCES conflation_clusters(id),
        FOREIGN KEY(curie_id) REFERENCES identifiers(id),
        UNIQUE(cluster_id, curie_id))
    
CREATE INDEX idx_cliques_type_id ON cliques (type_id);
CREATE INDEX idx_cliques_primary_identifier_id ON cliques (primary_identifier_id);
CREATE INDEX idx_identifiers_descriptions_description_id ON identifiers_descriptions (description_id);
CREATE INDEX idx_identifiers_descriptions_identifier_id ON identifiers_descriptions (identifier_id);
CREATE INDEX idx_identifiers_cliques_identifier_id ON identifiers_cliques (identifier_id);
CREATE INDEX idx_identifiers_cliques_clique_id ON identifiers_cliques (clique_id);
CREATE INDEX idx_identifiers_taxa_identifier_id ON identifiers_taxa (identifier_id);
CREATE INDEX idx_identifiers_taxa_taxa_identifier_id ON identifiers_taxa (taxa_identifier_id);
CREATE INDEX idx_conflation_members_curie_id ON conflation_members (curie_id);
CREATE INDEX idx_conflation_clusters_type ON conflation_clusters (type);
