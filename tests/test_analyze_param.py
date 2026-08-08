from apps.home import analyze_param


def test_extracts_where_parameters_with_postgresql_constructs():
    query = """
        SELECT *
          FROM public.orders AS o
         WHERE o.id = $1
           AND o.status IN ($2, $3)
           AND o.created_at BETWEEN $4::date AND $5
           AND $6 = o.customer_id
    """

    assert analyze_param.extract_parameter_columns(query) == {
        "1": "orders.id",
        "2": "orders.status",
        "3": "orders.status",
        "4": "orders.created_at",
        "5": "orders.created_at",
        "6": "orders.customer_id",
    }


def test_extracts_update_assignment_and_where_parameters():
    query = """
        UPDATE public.orders AS o
           SET status = $1,
               note = concat(note, $2)
         WHERE o.id = $3
    """

    assert analyze_param.extract_parameter_columns(query) == {
        "1": "orders.status",
        "2": "orders.note",
        "3": "orders.id",
    }


def test_extracts_insert_values_parameters():
    query = """
        INSERT INTO public.orders (id, status)
        VALUES ($1, $2), ($3, $4)
    """

    assert analyze_param.extract_parameter_columns(query) == {
        "1": "orders.id",
        "2": "orders.status",
        "3": "orders.id",
        "4": "orders.status",
    }


def test_extracts_parameters_in_subquery_scope():
    query = """
        SELECT *
          FROM customers AS c
         WHERE c.region = $1
           AND EXISTS (
               SELECT 1
                 FROM orders AS o
                WHERE o.customer_id = c.id
                  AND o.status = $2
           )
    """

    assert analyze_param.extract_parameter_columns(query) == {
        "1": "customers.region",
        "2": "orders.status",
    }


def test_extracts_select_projection_parameters():
    query = "SELECT $1::regclass AS classid, coalesce(o.name, $2) AS name FROM orders o"

    assert analyze_param.extract_parameter_columns(query) == {
        "1": "orders.classid",
        "2": "orders.name",
    }


def test_falls_back_to_sqlglot_when_pglast_rejects_query(monkeypatch):
    monkeypatch.setattr(
        analyze_param,
        "_extract_parameter_columns_pglast",
        lambda _query: (_ for _ in ()).throw(analyze_param.PglastParseError("bad SQL")),
    )
    monkeypatch.setattr(
        analyze_param,
        "_extract_parameter_columns_sqlglot",
        lambda _query: {"1": "fallback.value"},
    )

    assert analyze_param.extract_parameter_columns("SELECT $1") == {
        "1": "fallback.value"
    }


def test_extracts_physical_tables_and_ignores_cte_names():
    query = """
        WITH recent_orders AS (
            SELECT order_id FROM sales.orders WHERE order_date >= $1
        )
        SELECT ro.order_id
          FROM recent_orders AS ro
          JOIN public.order_details AS od USING (order_id)
    """

    assert analyze_param.extract_referenced_tables(query) == [
        "public.order_details",
        "sales.orders",
    ]


def test_matches_qualified_and_unqualified_table_references():
    assert analyze_param.query_references_table(
        "SELECT * FROM orders WHERE customer_id = $1", "public", "orders"
    )
    assert analyze_param.query_references_table(
        "SELECT * FROM public.orders WHERE customer_id = $1", "public", "orders"
    )
    assert not analyze_param.query_references_table(
        "SELECT * FROM archive.orders", "public", "orders"
    )


def test_safe_table_extraction_returns_empty_list_for_invalid_sql():
    assert analyze_param.extract_referenced_tables_safe(
        "SELECT * FROM orders WHERE ("
    ) == []
