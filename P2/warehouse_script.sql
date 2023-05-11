create table hospital
(
    id            integer not null
        primary key,
    name          varchar(100),
    county        varchar(100),
    planning_area integer,
    control_type  varchar(100),
    type          varchar(100),
    phone         varchar(100),
    address       varchar(100),
    city          varchar(100),
    zipcode       varchar(100),
    ceo           varchar(100)
);

create table expenses
(
    id           integer not null
        primary key,
    operational  bigint,
    professional integer
);

create table outpatient_revenue
(
    id                   integer not null
        primary key,
    medicare_traditional integer,
    medicare_care        integer,
    medi_traditional     integer,
    medi_care            integer
);

create table discharges
(
    id                   integer not null
        primary key,
    medicare_traditional integer,
    medicare_care        integer,
    medi_traditional     integer,
    medi_care            integer
);

create table utilization
(
    id             integer not null
        primary key,
    available_beds integer,
    staffed_beds   integer,
    license_beds   integer
);

create table visits
(
    id                   integer not null
        primary key,
    medicare_traditional integer,
    medicare_care        integer,
    medi_traditional     integer,
    medi_care            integer
);


create table patient_days
(
    id                   integer not null
        primary key,
    medicare_traditional integer,
    medicare_care        integer,
    medi_traditional     integer,
    medi_care            integer
);


create table inpatient_revenue
(
    id                   integer not null
        primary key,
    medicare_traditional integer,
    medicare_care        integer,
    medi_traditional     integer,
    medi_care            integer
);

create table report
(
    id         integer not null
        primary key,
    year       integer,
    quarter    integer,
    start_date date,
    end_date   date
);


create table staff
(
    id                           integer
        primary key,
    hid                          integer
        constraint fk_hid
            references hospital,
    productive_hours_per_patient integer,
    productive_hours             integer,
    position                     varchar(100)
);


create table report_content
(
    id             serial
        primary key,
    current_status varchar(100),
    rid            integer
        constraint fk_rid
            references report,
    hid            integer
        constraint fk_hid2
            references hospital,
    outid          integer
        constraint fk_out
            references outpatient_revenue,
    visid          integer
        constraint fk_visid
            references visits,
    expid          integer
        constraint fk_expid
            references expenses,
    inid           integer
        constraint fk_inid
            references inpatient_revenue,
    utilid         integer
        constraint fk_utilid
            references utilization,
    patid          integer
        constraint fk_patid
            references patient_days,
    disid          integer
        constraint fk_disid
            references discharges
);
