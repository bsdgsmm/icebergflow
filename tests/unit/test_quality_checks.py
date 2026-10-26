from icebergflow.quality.checks import (
    ERROR,
    WARN,
    accepted_values,
    equal,
    expression,
    not_null,
    relationship,
    unique,
)
from icebergflow.quality.suite import gold_checks, silver_checks


def test_unique_groups_by_all_keys():
    check = unique("silver.order_items", ["order_id", "line_number"])
    assert check.name == "unique:silver.order_items(order_id, line_number)"
    assert "GROUP BY order_id, line_number HAVING COUNT(*) > 1" in check.sql
    assert check.severity == ERROR


def test_not_null_creates_one_check_per_column():
    checks = not_null("t", ["a", "b"], severity=WARN)
    assert [c.name for c in checks] == ["not_null:t.a", "not_null:t.b"]
    assert all(c.severity == WARN for c in checks)
    assert checks[1].sql == "SELECT COUNT(*) FROM t WHERE b IS NULL"


def test_accepted_values_quotes_and_handles_nulls():
    strict = accepted_values("t", "status", ["placed", "o'brien"])
    lenient = accepted_values("t", "status", ["placed"], allow_null=True)
    assert "NOT IN ('placed', 'o''brien') OR status IS NULL" in strict.sql
    assert lenient.sql.endswith("NOT IN ('placed')")


def test_relationship_uses_anti_join_and_ignores_null_keys():
    check = relationship("silver.orders", "customer_id", "silver.customers", "customer_id")
    assert "LEFT ANTI JOIN silver.customers p ON c.customer_id = p.customer_id" in check.sql
    assert check.sql.endswith("WHERE c.customer_id IS NOT NULL")


def test_expression_treats_null_as_violation():
    check = expression("t", "quantity > 0", "positive_quantity")
    assert check.sql == "SELECT COUNT(*) FROM t WHERE NOT COALESCE(quantity > 0, FALSE)"


def test_equal_compares_null_safely():
    check = equal("rows", "t", "SELECT 1", "SELECT 2")
    assert "l.v <=> r.v" in check.sql


def test_default_suites_have_unique_check_names():
    for suite in (silver_checks(), gold_checks()):
        names = [c.name for c in suite]
        assert len(names) == len(set(names))
        assert all(c.severity in {ERROR, WARN} for c in suite)
