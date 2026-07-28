create table if not exists common_city_context (
    subsidiary_name text not null,
    strategic_city_name text not null,
    source_strategic_city_name text not null,
    context_version text not null,
    policy_version text not null,
    verification_only boolean not null default true check (verification_only),
    context_status text not null default 'candidate'
        check (context_status in ('candidate', 'reviewed', 'active', 'retired')),
    activation_revision bigint not null default 0 check (activation_revision >= 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name)
);

create table if not exists common_region_plan (
    subsidiary_name text not null,
    strategic_city_name text not null,
    plan_id text not null,
    schema_version text not null,
    policy_version text not null,
    verification_only boolean not null default true check (verification_only),
    source_file_name text not null,
    source_sha256 text not null check (source_sha256 ~ '^[0-9a-f]{64}$'),
    manifest_sha256 text check (manifest_sha256 is null or manifest_sha256 ~ '^[0-9a-f]{64}$'),
    bundle_sha256 text check (bundle_sha256 is null or bundle_sha256 ~ '^[0-9a-f]{64}$'),
    fixed_region_sha256 text check (fixed_region_sha256 is null or fixed_region_sha256 ~ '^[0-9a-f]{64}$'),
    boundary_policy_sha256 text check (boundary_policy_sha256 is null or boundary_policy_sha256 ~ '^[0-9a-f]{64}$'),
    technician_policy_sha256 text check (technician_policy_sha256 is null or technician_policy_sha256 ~ '^[0-9a-f]{64}$'),
    plan_status text not null default 'candidate'
        check (plan_status in ('candidate', 'reviewed', 'active', 'superseded', 'rejected')),
    revision bigint not null default 0 check (revision >= 0),
    membership_input_rows integer not null check (membership_input_rows >= 0),
    membership_accepted_rows integer not null check (membership_accepted_rows >= 0),
    membership_rejected_rows integer not null check (membership_rejected_rows >= 0),
    unique_postal_count integer not null check (unique_postal_count > 0),
    technician_count integer not null check (technician_count > 0),
    ambiguous_postal_count integer not null check (ambiguous_postal_count >= 0),
    import_idempotency_key text not null,
    imported_by text not null,
    reviewed_by text,
    review_reference text,
    reviewed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, plan_id),
    unique (subsidiary_name, strategic_city_name, import_idempotency_key),
    foreign key (subsidiary_name, strategic_city_name)
        references common_city_context (subsidiary_name, strategic_city_name),
    check (membership_input_rows = membership_accepted_rows + membership_rejected_rows),
    check (
        plan_status in ('candidate', 'rejected')
        or (
            manifest_sha256 is not null
            and bundle_sha256 is not null
            and fixed_region_sha256 is not null
            and boundary_policy_sha256 is not null
            and technician_policy_sha256 is not null
        )
    )
);

create table if not exists common_region_plan_region (
    subsidiary_name text not null,
    strategic_city_name text not null,
    plan_id text not null,
    region_seq integer not null check (region_seq between 1 and 6),
    region_id text not null,
    region_name text not null,
    source_territory text not null,
    created_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, plan_id, region_seq),
    unique (subsidiary_name, strategic_city_name, plan_id, region_id),
    foreign key (subsidiary_name, strategic_city_name, plan_id)
        references common_region_plan (subsidiary_name, strategic_city_name, plan_id)
);

create table if not exists common_region_plan_postal (
    subsidiary_name text not null,
    strategic_city_name text not null,
    plan_id text not null,
    postal_code text not null check (postal_code ~ '^[0-9]{5}$'),
    region_seq integer,
    area_type text not null check (area_type in ('DMS', 'DMS2')),
    source_membership_count integer not null default 1
        check (source_membership_count in (1, 2)),
    resolution_status text not null default 'not_required'
        check (resolution_status in ('not_required', 'pending', 'resolved')),
    source_region_seqs jsonb not null,
    resolution_metadata jsonb,
    created_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, plan_id, postal_code),
    unique (subsidiary_name, strategic_city_name, plan_id, postal_code, region_seq),
    foreign key (subsidiary_name, strategic_city_name, plan_id, region_seq)
        references common_region_plan_region (
            subsidiary_name, strategic_city_name, plan_id, region_seq
        ),
    check (
        (source_membership_count = 1 and region_seq is not null and resolution_status = 'not_required')
        or (
            source_membership_count = 2
            and (
                (resolution_status = 'pending' and region_seq is null)
                or (resolution_status = 'resolved' and region_seq is not null)
            )
        )
    )
);

create table if not exists common_region_plan_technician (
    subsidiary_name text not null,
    strategic_city_name text not null,
    plan_id text not null,
    employee_code text not null check (employee_code ~ '^AI[0-9]{6}$'),
    assigned_region_seq integer not null,
    policy_mode text not null
        check (policy_mode = 'assigned_region_boundary_spillover'),
    active_flag boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, plan_id, employee_code),
    foreign key (subsidiary_name, strategic_city_name, plan_id, assigned_region_seq)
        references common_region_plan_region (
            subsidiary_name, strategic_city_name, plan_id, region_seq
        )
);

create table if not exists common_region_plan_boundary_overflow (
    subsidiary_name text not null,
    strategic_city_name text not null,
    plan_id text not null,
    postal_code text not null,
    primary_region_seq integer not null,
    alternate_region_seq integer not null,
    allow_overflow boolean not null,
    penalty_cost integer,
    rationale text,
    policy_version text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, plan_id, postal_code),
    foreign key (
        subsidiary_name, strategic_city_name, plan_id, postal_code, primary_region_seq
    ) references common_region_plan_postal (
        subsidiary_name, strategic_city_name, plan_id, postal_code, region_seq
    ),
    foreign key (subsidiary_name, strategic_city_name, plan_id, alternate_region_seq)
        references common_region_plan_region (
            subsidiary_name, strategic_city_name, plan_id, region_seq
        ),
    check (primary_region_seq <> alternate_region_seq),
    check (
        (allow_overflow and penalty_cost = 4500)
        or (not allow_overflow and penalty_cost is null)
    )
);

create table if not exists common_region_plan_activation (
    subsidiary_name text not null,
    strategic_city_name text not null,
    activation_revision bigint not null check (activation_revision > 0),
    plan_id text not null,
    plan_revision bigint not null check (plan_revision >= 0),
    verification_only boolean not null default true check (verification_only),
    active_flag boolean not null default true,
    preview_digest text not null check (preview_digest ~ '^[0-9a-f]{64}$'),
    idempotency_key text not null,
    activated_by text not null,
    activation_reference text not null,
    activated_at timestamptz not null default now(),
    superseded_at timestamptz,
    created_at timestamptz not null default now(),
    primary key (subsidiary_name, strategic_city_name, activation_revision),
    unique (subsidiary_name, strategic_city_name, idempotency_key),
    foreign key (subsidiary_name, strategic_city_name, plan_id)
        references common_region_plan (subsidiary_name, strategic_city_name, plan_id),
    check (
        (active_flag and superseded_at is null)
        or (not active_flag and superseded_at is not null)
    )
);

create index if not exists common_region_plan_status_idx
    on common_region_plan (subsidiary_name, strategic_city_name, plan_status, updated_at);

create index if not exists common_region_plan_postal_region_idx
    on common_region_plan_postal (subsidiary_name, strategic_city_name, plan_id, region_seq);

create index if not exists common_region_plan_technician_region_idx
    on common_region_plan_technician (
        subsidiary_name, strategic_city_name, plan_id, assigned_region_seq
    );

create unique index if not exists common_region_plan_one_active_idx
    on common_region_plan_activation (subsidiary_name, strategic_city_name)
    where active_flag;

create index if not exists common_region_plan_activation_plan_idx
    on common_region_plan_activation (
        subsidiary_name, strategic_city_name, plan_id, activation_revision desc
    );
