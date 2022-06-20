DROP SCHEMA IF EXISTS `region_home_school`;
CREATE SCHEMA `region_home_school`;
USE `region_home_school`;

DROP TABLE IF EXISTS `school_ratings`;
DROP TABLE IF EXISTS `education_phases`;
DROP TABLE IF EXISTS `schools`;

DROP TABLE IF EXISTS `property_transactions`;
DROP TABLE IF EXISTS `tenures`;
DROP TABLE IF EXISTS `properties`;

DROP TABLE IF EXISTS `postcodes`;
DROP TABLE IF EXISTS `postgroups`;
DROP TABLE IF EXISTS `places`;


CREATE TABLE `places` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_places PRIMARY KEY (`id`),
    UNIQUE INDEX ix_places_name (`name`)
);

CREATE TABLE `postgroups` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(5)      NOT NULL,
    CONSTRAINT pk_postgroups PRIMARY KEY (`id`),
    UNIQUE INDEX ix_postgroups_name (`name`)
);

CREATE TABLE `places_postgroups` (
    place_id                    INT             NOT NULL,
    postgroup_id                INT             NOT NULL,
    CONSTRAINT pk_places_postgroups PRIMARY KEY (`place_id`, `postgroup_id`)
);

CREATE TABLE `postcodes` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    postgroup_id                INT             NOT NULL,
    name                        VARCHAR(10)      NOT NULL,
    CONSTRAINT pk_postcodes PRIMARY KEY (`id`),
    CONSTRAINT fk_postcodes_postgroup_id FOREIGN KEY (`postgroup_id`) REFERENCES `postgroups` (`id`),
    UNIQUE INDEX ix_postgroups_name (`name`)
);

CREATE TABLE `property_types` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_property_types PRIMARY KEY (`id`),
    INDEX ix_property_types_name (`name`)
);

CREATE TABLE `properties` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    number_or_name              VARCHAR(120)    NOT NULL,
    building_ref                VARCHAR(120)    NOT NULL,
    street_name                 VARCHAR(120)    NOT NULL,
    postcode_id                 INT             NOT NULL,
    property_type_id            INT             NOT NULL,
    CONSTRAINT pk_properties PRIMARY KEY (`id`),
    CONSTRAINT fk_properties_postcode_id FOREIGN KEY (`postcode_id`) REFERENCES `postcodes` (`id`),
    CONSTRAINT fk_properties_property_type_id FOREIGN KEY (`property_type_id`) REFERENCES `property_types` (`id`),
    UNIQUE INDEX ix_properties (`number_or_name`, `building_ref`, `street_name`, `postcode_id`, `property_type_id`)
);

CREATE TABLE `tenures` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_tenures PRIMARY KEY (`id`),
    INDEX ix_tenures_name (`name`)
);

CREATE TABLE `property_transactions` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    property_id                 INT             NOT NULL,
    new_build                   BOOLEAN         NOT NULL,
    tenure_id                   INT             NOT NULL,
    price                       DECIMAL         NOT NULL,
    ts                          TIMESTAMP       NOT NULL,
    CONSTRAINT pk_property_transactions PRIMARY KEY (`id`),
    CONSTRAINT fk_property_transactions_tenure FOREIGN KEY (`tenure_id`) REFERENCES `tenures` (`id`),
    CONSTRAINT fk_property_transactions_property_id FOREIGN KEY (`property_id`) REFERENCES `properties` (`id`),
    UNIQUE INDEX ix_property_transactions (`property_id`, `new_build`, `tenure_id`, `ts`)
);

CREATE TABLE `education_phases` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_education_phases PRIMARY KEY (`id`),
    UNIQUE INDEX ix_education_phases_name (`name`)
);

CREATE TABLE `schools` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    name                        VARCHAR(120)    NOT NULL,
    postcode_id                 INT             NOT NULL,
    CONSTRAINT pk_schools PRIMARY KEY (`id`),
    CONSTRAINT fk_schools_postcode_id FOREIGN KEY (`postcode_id`) REFERENCES `postcodes` (`id`),
    UNIQUE INDEX ix_school (`name`, `postcode_id`)
);

CREATE TABLE `school_ratings` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    school_id                   INT             NOT NULL,
    education_phase_id          INT             NOT NULL,
    rating                      FLOAT           NOT NULL,
    ts                          TIMESTAMP       NOT NULL,
    CONSTRAINT pk_school_ratings PRIMARY KEY (`id`),
    CONSTRAINT fk_school_ratings_school_id FOREIGN KEY (`school_id`) REFERENCES `schools` (`id`),
    CONSTRAINT fk_school_ratings_education_phase_id FOREIGN KEY (`education_phase_id`) REFERENCES `education_phases` (`id`),
    UNIQUE INDEX ix_school_ratings (`school_id`, `education_phase_id`, `rating`, `ts`)
);