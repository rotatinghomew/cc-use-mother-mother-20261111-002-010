from __future__ import annotations
import json
import uuid
from pathlib import Path
from typing import Any, Mapping
from screening_kpi_auditor.runtime import DomainError, ProcessingContext, SQLiteLedger, fingerprint, normalize_text
from screening_kpi_auditor.contracts.indicator_spec import build_processor as indicator_spec_factory
from screening_kpi_auditor.ingestion.report_batch import build_processor as report_batch_factory
from screening_kpi_auditor.denominators.population import build_processor as population_factory
from screening_kpi_auditor.reconciliation.period_close import build_processor as period_close_factory
from screening_kpi_auditor.anomalies.change_points import build_processor as change_points_factory
from screening_kpi_auditor.sampling.traceback import build_processor as traceback_factory
from screening_kpi_auditor.attestation.signoff import build_processor as signoff_factory
from screening_kpi_auditor.findings.audit_report import build_processor as audit_report_factory

PROCESSOR_FACTORIES = [indicator_spec_factory, report_batch_factory, population_factory, period_close_factory, change_points_factory, traceback_factory, signoff_factory, audit_report_factory]

class ApplicationService:
    def __init__(self, database: str | Path):
        self.ledger=SQLiteLedger(database)
        self.processors=[factory() for factory in PROCESSOR_FACTORIES]

    def validate_input(self, record: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(record, Mapping): raise DomainError("record must be object")
        result=dict(record)
        result["case_id"]=normalize_text(result.get("case_id"),"case_id",96)
        if not result["case_id"]: raise DomainError("case_id is required")
        result.setdefault("subject_id",result["case_id"])
        result.setdefault("category","screening")
        result.setdefault("region","synthetic-region")
        result.setdefault("event_date","2026-11-11")
        result.setdefault("status","new")
        result.setdefault("count",1)
        result.setdefault("total",1)
        result.setdefault("attributes",{"source":"synthetic"})
        result.setdefault("tags",["offline","fixture"])
        result.setdefault("source_system","local-fixture")
        result.setdefault("revision",0)
        return result

    def execute(self, record: Mapping[str, Any], request_key: str | None=None) -> dict[str, Any]:
        normalized=self.validate_input(record)
        normalized=self.architecture_preflight(normalized)
        key=request_key or fingerprint(normalized)
        recalled=self.ledger.recalled(key)
        if recalled is not None: return recalled | {"replayed":True}
        ctx=ProcessingContext(uuid.uuid4().hex,normalized["case_id"],normalized)
        ctx.emit("received","run_started",project='screening_kpi_auditor')
        try:
            for processor in self.architecture_plan(ctx):
                processor.process(ctx)
                self.ledger.save(ctx)
        except Exception as exc:
            self.ledger.dead_letter(ctx,ctx.state,str(exc),normalized)
            raise
        self.architecture_finalize(ctx)
        ctx.advance("completed")
        self.ledger.save(ctx)
        response={"project":'screening_kpi_auditor',"run_id":ctx.run_id,"case_id":ctx.case_id,"state":ctx.state,
                  "revision":ctx.revision,"flags":ctx.flags,"metrics":ctx.metrics,"evidence_count":len(ctx.evidence),
                  "event_count":len(ctx.events),"payload":ctx.payload,"replayed":False}
        self.ledger.remember(key,ctx.run_id,response)
        return response

    def architecture_preflight(self, record):
        record=dict(record)
        numerator=max(0,float(record.get("count",0))); denominator=max(0,float(record.get("total",0)))
        if numerator > denominator and denominator > 0: raise DomainError("numerator exceeds denominator")
        record["audit_workpaper"]={"numerator":numerator,"denominator":denominator,"rate":(numerator/denominator if denominator else None),"backfills":[],"findings":[]}
        return record

    def architecture_plan(self, ctx):
        return list(self.processors)

    def architecture_finalize(self, ctx):
        paper=ctx.payload["audit_workpaper"]
        rate=paper["rate"]
        if rate is None: paper["findings"].append("missing-denominator")
        elif rate > 0.95: paper["findings"].append("high-coverage-review")
        elif rate < 0.2: paper["findings"].append("low-coverage-review")
        paper["attestation"]=fingerprint({"case":ctx.case_id,"rate":rate,"evidence":ctx.evidence})
        ctx.metrics["audited_rate"]=-1 if rate is None else round(rate,6)
        ctx.emit("audit_findings","workpaper_signed",finding_count=len(paper["findings"]))

    def execute_many(self, records: list[Mapping[str, Any]]) -> dict[str, Any]:
        outputs=[]; failures=[]
        for index, record in enumerate(records):
            try: outputs.append(self.execute(record,request_key=f"batch:{index}:{fingerprint(record)}"))
            except Exception as exc: failures.append({"index":index,"error":str(exc)})
        return {"accepted":outputs,"failures":failures,"accepted_count":len(outputs),"failure_count":len(failures)}

def run_fixture(fixture: str | Path, database: str | Path) -> dict[str, Any]:
    data=json.loads(Path(fixture).read_text(encoding="utf-8"))
    service=ApplicationService(database)
    if isinstance(data,list): return service.execute_many(data)
    return service.execute(data)
