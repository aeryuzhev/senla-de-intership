-- -------------------------------------------------------------------------------------
drop table if exists stg_articles;
-- -------------------------------------------------------------------------------------
create table stg_articles (
    article_id                    integer,
    product_code                  integer,
    prod_name                     text,
    product_type_no               smallint,
    product_type_name             text,
    product_group_name            text,
    graphical_appearance_no       integer,
    graphical_appearance_name     text,
    colour_group_code             smallint,
    colour_group_name             text,
    perceived_colour_value_id     smallint,
    perceived_colour_value_name   text,
    perceived_colour_master_id    smallint,
    perceived_colour_master_name  text,
    department_no                 smallint,
    department_name               text,
    index_code                    text, 
    index_name                    text,
    index_group_no                smallint,
    index_group_name              text, 
    section_no                    smallint,
    section_name                  text,  
    garment_group_no              smallint,
    garment_group_name            text,
    detail_desc                   text
);
-- -------------------------------------------------------------------------------------