-- CKAN application + Datastore databases (PoC localhost).
CREATE USER ckandbuser WITH PASSWORD 'ckan_local_pwd';
CREATE DATABASE ckandb OWNER ckandbuser ENCODING 'UTF8' TEMPLATE template0;
CREATE USER datastore_ro WITH PASSWORD 'datastore_ro_pwd';
CREATE DATABASE datastore OWNER ckandbuser ENCODING 'UTF8' TEMPLATE template0;
