DROP TABLE `transactions`;
DROP TABLE `properties`;
DROP TABLE `municipalities`;
DROP TABLE `districts`;
DROP TABLE `counties`;
DROP TABLE `leasetypes`;

CREATE TABLE `leasetypes` (
    id                          INT             NOT NULL    AUTO_INCREMENT;
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_leasetypes PRIMARY KEY (`id`),
    INDEX ix_leasetypes_name (`name`)
);

CREATE TABLE `counties` (
    id                          INT             NOT NULL    AUTO_INCREMENT;
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_countries PRIMARY KEY (`id`),
    INDEX ix_countries_name (`name`)
);

CREATE TABLE `districts` (
    id                          INT             NOT NULL    AUTO_INCREMENT;
    county_id                   INT             NOT NULL,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_districts PRIMARY KEY (`id`),
    CONSTRAINT fk_districts_county_id FOREIGN KEY (`county_id`) REFERENCES `counties` (`id`),
    INDEX ix_districts_county_name (`county_id`, `name`)
);

CREATE TABLE `municipalities` (
    id                          INT             NOT NULL    AUTO_INCREMENT;
    district_id                 INT             NOT NULL,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_municipalities PRIMARY KEY (`id`),
    CONSTRAINT fk_municipalities_district_id FOREIGN KEY (`district_id`) REFERENCES `districts` (`id`),
    INDEX ix_municipalities_district_name (`district_id`, `name`)
);

CREATE TABLE `localities` (
    id                          INT             NOT NULL    AUTO_INCREMENT;
    municipality_id             INT             NOT NULL,
    name                        VARCHAR(120)    NOT NULL,
    CONSTRAINT pk_localities PRIMARY KEY (`id`),
    CONSTRAINT fk_localities_district_id FOREIGN KEY (`municipality_id`) REFERENCES `municipalities` (`id`),
    INDEX ix_localities_district_name (`municipality_id`, `name`)
);

CREATE TABLE `properties` (
    id                          INT             NOT NULL,
    property_number_or_name     VARCHAR(125)    NOT NULL,
    building_or_block           VARCHAR(125)    NOT NULL,
    street_name                 VARCHAR(125)    NOT NULL,
    postgroup                   VARCHAR(5)      NOT NULL,
    postcode                    VARCHAR(10)     NOT NULL,
    locality_id                 INT             NOT NULL,
    municipality_id             INT             NOT NULL,
    district_id                 INT             NOT NULL,
    CONSTRAINT pk_properties PRIMARY KEY (`id`),
    CONSTRAINT fk_properties_municipality_id FOREIGN KEY (`municipality_id`) REFERENCES `municipalities` (`id`),
    CONSTRAINT fk_properties_district_id FOREIGN KEY (`district_id`) REFERENCES `districts` (`id`),
    CONSTRAINT fk_properties_county_id FOREIGN KEY (`county_id`) REFERENCES `counties` (`id`),
    UNIQUE INDEX ix_properties_address (`property_number_or_name`, `building_or_block`, `street_name`, `postcode`, `locality_id`, `municipality_id`, `district_id`),
    INDEX ix_property_postgroup (`postgroup`)
);

CREATE TABLE `transactions` (
    id                          INT             NOT NULL    AUTO_INCREMENT,
    property_id                 INT             NOT NULL,
    new_build                   BOOLEAN         NOT NULL,
    lease_type                  INT             NOT NULL,
    price                       DECIMAL         NOT NULL,
    ts                          timestamp       NOT NULL,
    CONSTRAINT pk_transactions PRIMARY KEY (`id`),
    CONSTRAINT fk_transactions_property_id FOREIGN KEY (`property_id`) REFERENCES `properties` (`id`),
);