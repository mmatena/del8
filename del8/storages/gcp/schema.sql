-- All tables have a uuid field that represents the uuid of
-- of that row.
--
-- Note that all uuid columns have a length of 32 characters
-- since the length of a uuid4 hex string is 32. Thus we assume
-- that all uuids are of that form.

-- CREATE TABLE Groups (
--     -- This IS the group's uuid.
--     uuid char(32) NOT NULL PRIMARY KEY,

--     -- This is the group, serialized as JSON. This lets us
--     -- get an ExperimentGroup object with all of its
--     -- group-specific organization built-in just given its
--     -- group uuid.
--     serialized jsonb NOT NULL
-- );

CREATE TABLE Items (
    -- Recall that the `uuid` column represents the uuid of
    -- the row, NOT the run (which is represented by the
    -- `run_uuid` field.)
    uuid       char(32) NOT NULL PRIMARY KEY,

    group_uuid char(32) NOT NULL,
    exp_uuid   char(32) NOT NULL,
    run_uuid   char(32) NOT NULL,

    data       jsonb NOT NULL
);

CREATE TABLE Blobs (
    -- Recall that the `uuid` column represents the uuid of
    -- the row, NOT the run or group (which are represented
    -- by the `run_uuid` and `group_uuid` fields, respectively.)
    uuid                    char(32) NOT NULL PRIMARY KEY,

    -- These are mostly for bookkeeping purposes. For example, we can
    -- easily delete all the blobs associated with an experiment when
    -- we delete it.
    group_uuid              char(32) NOT NULL,
    exp_uuid                char(32) NOT NULL,
    run_uuid                char(32) NOT NULL,

    -- We can limit this to 1024 as cloud storage object names
    -- are limited to 1024 bytes.
    gcp_storage_object_name varchar(1024)
);

-- -- TODO: Figure out how to do what I'm trying to do in the next line.
-- GRANT CONNECT, SELECT, INSERT, UPDATE, DELETE ON DATABASE del8 TO del8;
