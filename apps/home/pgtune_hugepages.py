"""Read-only HugeTLB guidance for the connected instance's current configuration."""

PARAMETERS = (
    'shared_buffers', 'max_connections', 'shared_memory_type',
    'shared_memory_size', 'shared_memory_size_in_huge_pages',
    'huge_page_size', 'huge_pages', 'huge_pages_status',
)


def build_guidance(settings, pending_restart=False):
    """Keep current-instance sizing separate from host-wide pool provisioning."""
    try:
        required = int(settings.get('shared_memory_size_in_huge_pages', ''))
    except (TypeError, ValueError):
        required = 0
    recommended = (required * 105 + 99) // 100 if required > 0 else None
    actions = [
        'This sizing describes the connected instance only. If pgTune changes memory '
        'or connection settings, recalculate after applying them and restarting, or '
        'use the target PostgreSQL binary with the complete target configuration and '
        'postgres -C shared_memory_size_in_huge_pages before deployment.',
    ]
    if pending_restart:
        actions.append('Settings have pending restarts: this result does not size those future changes.')
    if settings.get('shared_memory_type') != 'mmap':
        actions.append('Linux HugeTLB requires shared_memory_type=mmap. Check the target '
                       'operating system and configuration before preparing the host.')
    if recommended:
        actions.append(f'Current instance: {required} pages required; reserve {recommended} '
                       'pages including 5% margin, rounded up. This margin is a pgAssistant policy.')
        if settings.get('huge_page_size') == '0':
            actions.append(f'On a Linux host dedicated to this single consumer, propose '
                           f'vm.nr_hugepages = {recommended} in /etc/sysctl.d/90-postgresql-hugepages.conf '
                           'and apply with sysctl --system. Confirm the default Hugepagesize in '
                           '/proc/meminfo first. On shared hosts, size the total pool for all '
                           'consumers; do not replace an existing larger pool with this value.')
        else:
            actions.append('Verify huge_page_size and configure the matching pool under '
                           '/sys/kernel/mm/hugepages/hugepages-<size>kB/. vm.nr_hugepages '
                           'only configures the default page size. Arrange persistence on the host.')
    else:
        actions.append('The required page count is unavailable or unsupported. Do not infer '
                       'a host reservation from shared_buffers alone; obtain the count '
                       'using the target PostgreSQL binary and configuration.')
    actions.extend([
        'Before reserving memory, check the host budget for the OS, filesystem cache '
        'and PostgreSQL private memory. Verify the actual allocated pool and available '
        'unreserved pages after applying: fragmentation can prevent full allocation. '
        'Check PostgreSQL user permissions (hugetlb_shm_group / memory-lock limits as applicable).',
        'For Docker, prepare the Linux host running the container (or its Linux VM), '
        'then verify container HugeTLB limits, parent cgroup limits and user permissions. '
        'shm_size does not reserve HugeTLB pages; do not put vm.nr_hugepages in Compose sysctls.',
        "Propose huge_pages = try in postgresql.conf, then restart PostgreSQL during a "
        'maintenance window. Check huge_pages_status = on if supported; otherwise inspect '
        'the PostgreSQL process HugeTLB mappings on the host. With try, startup can succeed '
        'using ordinary pages. Only choose huge_pages = on explicitly if startup failure '
        'on allocation failure is acceptable.',
        'Explicit HugeTLB pages and Transparent Huge Pages (THP) are different features; '
        'enabling THP is not part of this recommendation.',
    ])
    if settings.get('huge_pages_status') == 'on':
        actions.insert(0, 'Huge Pages are already active for the connected instance.')
    return {'settings': settings, 'pending_restart': pending_restart,
            'recommended_pages': recommended, 'actions': actions}


def inspect_huge_pages(connection):
    cursor = connection.cursor()
    try:
        # Missing settings on older PostgreSQL versions simply produce no row.
        cursor.execute(
            'SELECT name, current_setting(name), pending_restart FROM pg_settings '
            'WHERE name = ANY(%s) ORDER BY name', (list(PARAMETERS),),
        )
        rows = cursor.fetchall()
        cursor.execute('SELECT EXISTS (SELECT 1 FROM pg_settings WHERE pending_restart)')
        pending = cursor.fetchone()[0]
        return build_guidance({row[0]: row[1] for row in rows}, bool(pending))
    finally:
        cursor.close()
