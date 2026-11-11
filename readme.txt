测试：python3 -m unittest discover -s tests/kpi_audit -p 'test_*.py'
构建：python3 -m compileall -q screening_kpi_auditor
验收：python3 -m screening_kpi_auditor.main fixtures/demo.json --db /tmp/screening_kpi_auditor-acceptance.sqlite3
