create table hospital
(
    id            int          not null
        primary key,
    name          varchar(100) null,
    county        varchar(100) null,
    planning_area int          null,
    control_type  varchar(100) null,
    type          varchar(100) null,
    phone         varchar(100) null,
    address       varchar(100) null,
    city          varchar(100) null,
    zipcode       varchar(100) null,
    ceo           varchar(100) null,
    latest        int          null
);

create table report
(
    id         int auto_increment
        primary key,
    year       int  not null,
    quarter    int  not null,
    start_date date not null,
    end_date   date not null
);

create table report_content
(
    id             int auto_increment
        primary key,
    current_status varchar(100) not null,
    rid            int          not null,
    hid            int          not null,
    constraint fk_hid
        foreign key (hid) references hospital (id),
    constraint fk_rid
        foreign key (rid) references report (id)
);

create table discharges
(
    rcid                 int auto_increment
        primary key,
    medicare_traditional int not null,
    medicare_care        int not null,
    medi_traditional     int not null,
    medi_care            int not null,
    constraint fk_rcid7
        foreign key (rcid) references report_content (id)
);

create table expenses
(
    rcid         int auto_increment
        primary key,
    operational  bigint not null,
    professional int    not null,
    constraint fk_rcid
        foreign key (rcid) references report_content (id)
);

create table inpatient_revenue
(
    rcid                 int auto_increment
        primary key,
    medicare_traditional int not null,
    medicare_care        int not null,
    medi_traditional     int not null,
    medi_care            int not null,
    constraint fk_rcid2
        foreign key (rcid) references report_content (id)
);

create table outpatient_revenue
(
    rcid                 int auto_increment
        primary key,
    medicare_traditional int not null,
    medicare_care        int not null,
    medi_traditional     int not null,
    medi_care            int not null,
    constraint fk_rcid4
        foreign key (rcid) references report_content (id)
);

create table patient_days
(
    rcid                 int auto_increment
        primary key,
    medicare_traditional int not null,
    medicare_care        int not null,
    medi_traditional     int not null,
    medi_care            int not null,
    constraint fk_rcid6
        foreign key (rcid) references report_content (id)
);

create table staff
(
    id                           int auto_increment
        primary key,
    hid                          int          not null,
    productive_hours_per_patient float        not null,
    productive_hours             int          not null,
    position                     varchar(100) not null,
    constraint staff_ibfk_1
        foreign key (hid) references hospital (id)
);

create index hid
    on staff (hid);

create table utilization
(
    rcid           int auto_increment
        primary key,
    available_beds int not null,
    staffed_beds   int not null,
    license_beds   int not null,
    constraint fk_rcid3
        foreign key (rcid) references report_content (id)
);

create table visits
(
    rcid                 int auto_increment
        primary key,
    medicare_traditional int not null,
    medicare_care        int not null,
    medi_traditional     int not null,
    medi_care            int not null,
    constraint fk_rcid5
        foreign key (rcid) references report_content (id)
);


