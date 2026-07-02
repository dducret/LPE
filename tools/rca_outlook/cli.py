from __future__ import annotations

import argparse
import os
import textwrap


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check LPE Autodiscover gates and canonical EWS/MAPI/RPC readiness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Examples:
              LPE_RCA_PASSWORD='...' tools/rca_outlook_connectivity_check.py --ews-readiness --allow-mutating-fixtures
              LPE_RCA_PASSWORD='...' tools/rca_outlook_connectivity_check.py --outlook-rca-readiness --allow-mutating-fixtures
              tools/rca_outlook_connectivity_check.py --base-url https://l-p-e.ch --email test@l-p-e.ch --expected-service-host mail.l-p-e.ch --outlook-rca-readiness --allow-mutating-fixtures --insecure
            """
        ),
    )
    parser.add_argument("--base-url", default=os.getenv("LPE_RCA_BASE_URL"))
    parser.add_argument("--email", default=os.getenv("LPE_RCA_EMAIL"))
    parser.add_argument("--password", default=os.getenv("LPE_RCA_PASSWORD"))
    parser.add_argument("--timeout", type=int, default=int(os.getenv("LPE_RCA_TIMEOUT", "20")))
    parser.add_argument(
        "--ews-readiness",
        action="store_true",
        help="Require the narrower EWS publication gate: EWS autodiscover, EWS auth, and live canonical EWS mail/contact/calendar fixtures.",
    )
    parser.add_argument(
        "--outlook-rca-readiness",
        action="store_true",
        help="Require the full Outlook RCA gate: EWS, EXCH, EXPR, mapiHttp, RPC proxy, NSPI fixture, and EMSMDB canonical Sent proof.",
    )
    parser.add_argument(
        "--allow-mutating-fixtures",
        action="store_true",
        help="Permit creation and cleanup of temporary mail, contact, and calendar fixtures in the target mailbox.",
    )
    parser.add_argument("--expect-ews", action="store_true", help="Require EWS discovery to be published.")
    parser.add_argument(
        "--expected-service-host",
        help="Require discovered service URLs and JMAP redirects to use this host, for example mail.l-p-e.ch.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification for local or pre-production smoke checks.",
    )
    parser.add_argument(
        "--expect-exchange-providers",
        action="store_true",
        help="Require both POX legacy EXCH and EXPR providers for RCA Outlook Anywhere validation.",
    )
    parser.add_argument(
        "--expect-exch-provider",
        action="store_true",
        help="Require the POX legacy EXCH provider without requiring EXPR.",
    )
    parser.add_argument(
        "--expect-expr-provider",
        action="store_true",
        help="Require the POX legacy EXPR provider without requiring EXCH.",
    )
    parser.add_argument("--expect-mapi", action="store_true", help="Require MAPI/HTTP discovery to be published.")
    parser.add_argument("--check-ews-basic", action="store_true", help="Exercise Basic auth against /EWS/Exchange.asmx.")
    parser.add_argument(
        "--check-live-fixtures",
        action="store_true",
        help="Create/read/delete live EWS message, Sent, contact, calendar fixtures and prove the authenticated mailbox plus contact through MAPI/NSPI.",
    )
    parser.add_argument(
        "--send-recipient",
        default=os.getenv("LPE_RCA_SEND_RECIPIENT"),
        help="Recipient for the canonical send/Sent live fixture. Defaults to --email.",
    )
    parser.add_argument("--check-mapi-ping", action="store_true", help="Exercise Basic auth PING against /mapi/emsmdb and /mapi/nspi.")
    parser.add_argument(
        "--check-mapi-nspi-bind-octet-stream",
        action="store_true",
        help="Exercise RCA-style NSPI Bind with Content-Type application/octet-stream.",
    )
    parser.add_argument(
        "--check-mapi-nspi-address-book",
        action="store_true",
        help="Exercise RCA-style NSPI address-book Check Name operations.",
    )
    parser.add_argument(
        "--check-mapi-empty-deleted-items",
        action="store_true",
        help="Create a temporary EWS message in Deleted Items, empty Deleted Items through MAPI EmptyFolder, and verify disappearance. Requires --dangerously-empty-deleted-items because it empties the target mailbox Deleted Items folder.",
    )
    parser.add_argument(
        "--dangerously-empty-deleted-items",
        action="store_true",
        help="Acknowledge that --check-mapi-empty-deleted-items empties the whole target mailbox Deleted Items folder.",
    )
    parser.add_argument(
        "--check-rpc-proxy-auth",
        action="store_true",
        help="Exercise RCA-style /rpc/rpcproxy.dll anonymous challenge and optional authenticated echo.",
    )
    parser.add_argument(
        "--check-rpc-proxy-mailstore-ping",
        action="store_true",
        help="Exercise RCA-style /rpc/rpcproxy.dll mailbox-store endpoint ping on :6001.",
    )
    return parser.parse_args()

