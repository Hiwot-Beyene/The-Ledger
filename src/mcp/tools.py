"""MCP command-side tools registered in :func:`mcp.server.build_fastmcp_server`.

Primary workflow tools include: ``submit_application``, ``request_credit_analysis``,
``start_agent_session``, ``record_credit_analysis``, ``record_fraud_screening``,
``initiate_compliance_check``, ``record_compliance_check``, ``generate_decision``,
``record_human_review``, plus ``approve_application``, ``run_integrity_check``,
``run_what_if_projection``, ``generate_regulatory_examination_package``.
"""

COMMAND_TOOL_NAMES = (
    "submit_application",
    "request_credit_analysis",
    "start_agent_session",
    "record_credit_analysis",
    "record_fraud_screening",
    "initiate_compliance_check",
    "record_compliance_check",
    "generate_decision",
    "record_human_review",
    "approve_application",
    "run_integrity_check",
    "run_what_if_projection",
    "generate_regulatory_examination_package",
)
