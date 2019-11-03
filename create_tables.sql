
CREATE TABLE taxo_documents (
    docuri text PRIMARY KEY,
    doc xml NOT NULL
);

CREATE TABLE entrypoints (
    name text PRIMARY KEY
);

CREATE TABLE entrypoint_docuris (
    entrypoint_name text NOT NULL REFERENCES entrypoints (name),
    docuri text NOT NULL REFERENCES taxo_documents,
    PRIMARY KEY (entrypoint_name, docuri)
);

CREATE TABLE dts_docuris (
    entrypoint_name text NOT NULL REFERENCES entrypoints (name),
    docuri text NOT NULL REFERENCES taxo_documents,
    PRIMARY KEY (entrypoint_name, docuri)
);

