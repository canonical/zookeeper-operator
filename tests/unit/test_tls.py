#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import cast
from unittest.mock import DEFAULT, Mock, PropertyMock, patch

import pytest
import yaml
from ops.testing import Container, Context, PeerRelation, Relation, Secret, State

from charm import ZooKeeperCharm
from literals import CERTS_REL_NAME, CONTAINER, PEER, SUBSTRATE, Status

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
ACTIONS = yaml.safe_load(Path("./actions.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())

TLS_NAME = "self-signed-certificates"

secret_suffix = "-k8s" if SUBSTRATE == "k8s" else ""


@pytest.fixture()
def base_state():

    if SUBSTRATE == "k8s":
        state = State(leader=True, containers=[Container(name=CONTAINER, can_connect=True)])

    else:
        state = State(leader=True)

    return state


@pytest.fixture()
def ctx() -> Context:
    ctx = Context(ZooKeeperCharm, meta=METADATA, config=CONFIG, actions=ACTIONS, unit_id=0)
    return ctx


def test_all_units_unified_succeeds(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_unit_data={"unified": "true"}, peers_data={})
    state_in = dataclasses.replace(base_state, relations=[cluster_peer])

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert charm.state.all_units_unified


def test_all_units_unified_fails(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER, PEER, local_unit_data={"unified": "true", "state": "started"}, peers_data={}
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer], planned_units=3)

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert not charm.state.all_units_unified


def test_all_units_unified_fails_if_not_all_units_related(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_unit_data={}, peers_data={})
    state_in = dataclasses.replace(base_state, relations=[cluster_peer], planned_units=3)

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert not charm.state.all_units_unified


def test_certificates_created_defers_if_not_stable(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with (
        patch("ops.framework.EventBase.defer") as patched,
        ctx(ctx.on.relation_joined(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert not charm.state.cluster.tls
        patched.assert_called_once()


def test_certificates_created_sets_upgrading_enabled(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with (
        patch("ops.framework.EventBase.defer"),
        patch(
            "core.cluster.ClusterState.stable",
            new_callable=PropertyMock,
            return_value=Status.ACTIVE,
        ),
        ctx(ctx.on.relation_created(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert charm.state.cluster.tls
        assert charm.state.cluster.switching_encryption


def test_certificates_joined_defers_if_disabled(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_app_data={}, local_unit_data={}, peers_data={})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME, remote_units_data={1: {}})
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with (
        patch("ops.framework.EventBase.defer") as patched,
        patch("core.cluster.ClusterState.stable", new_callable=PropertyMock, return_value=True),
        ctx(ctx.on.relation_joined(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        patched.assert_called_once()
        assert not charm.state.unit_server.private_key


def test_certificates_joined_creates_private_key_if_enabled(
    ctx: Context, base_state: State
) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_app_data={"tls": "enabled"},
        peers_data={},
        local_unit_data={
            "hostname": "treebeard",
            "ip": "1.1.1.1",
        },
    )
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME, remote_units_data={1: {}})
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with (
        patch("core.cluster.ClusterState.stable", new_callable=PropertyMock, return_value=True),
        ctx(ctx.on.relation_joined(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert charm.state.unit_server.private_key
        assert "BEGIN RSA PRIVATE KEY" in charm.state.unit_server.private_key.splitlines()[0]


def test_certificates_joined_creates_new_key_trust_store_password(
    ctx: Context, base_state: State
) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_app_data={"tls": "enabled"},
        local_unit_data={
            "hostname": "treebeard",
            "ip": "1.1.1.1",
        },
        peers_data={},
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer])

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert not charm.state.unit_server.keystore_password
        assert not charm.state.unit_server.truststore_password

    # Given
    tls_relation = Relation(
        CERTS_REL_NAME, TLS_NAME, remote_units_data={1: {}}, local_unit_data={}
    )
    state_in = dataclasses.replace(state_in, relations=[cluster_peer, tls_relation])

    # When
    with (
        patch("core.cluster.ClusterState.stable", new_callable=PropertyMock, return_value=True),
        patch("core.models.ZKCluster.tls", new_callable=PropertyMock, return_value=True),
        patch("core.models.ZKServer.host", new_callable=PropertyMock, return_value="host"),
        ctx(ctx.on.relation_joined(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert charm.state.unit_server.keystore_password
        assert charm.state.unit_server.truststore_password
        assert (
            charm.state.unit_server.keystore_password
            != charm.state.unit_server.truststore_password
        )


def test_certificates_available_fails_wrong_csr(ctx: Context, base_state: State) -> None:
    # NOTE: do we really need this test? The tls lib already takes care of checking the csr before
    # emitting certificate_available. If not, we can also remove the condition in event/tls.py
    # Given
    cluster_peer = PeerRelation(PEER, PEER, local_unit_data={"csr": "not-missing"})
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])
    mock_event = Mock()
    mock_event.certificate_signing_request = "missing"

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)
        charm.tls_events._on_certificate_available(mock_event)

        # Then
        assert not charm.state.unit_server.certificate
        assert not charm.state.unit_server.ca_cert


def test_certificates_available_succeeds(ctx: Context, base_state: State) -> None:
    # Given
    provider_data = {
        "certificates": json.dumps(
            [
                {
                    "certificate_signing_request": "not-missing",
                    "ca": "ca",
                    "certificate": "cert",
                    "chain": ["ca", "cert"],
                }
            ]
        )
    }
    requirer_data = {
        "certificate_signing_requests": json.dumps(
            [{"certificate_signing_request": "not-missing"}]
        )
    }
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_app_data={"tls": "enabled", "switching-encryption": "started"},
        local_unit_data={},
        peers_data={},
    )
    tls_relation = Relation(
        CERTS_REL_NAME, TLS_NAME, remote_app_data=provider_data, local_unit_data=requirer_data
    )
    tls_secret = Secret(
        {"csr": "not-missing"}, label=f"{PEER}.zookeeper{secret_suffix}.unit", owner="unit"
    )
    state_in = dataclasses.replace(
        base_state, relations=[cluster_peer, tls_relation], secrets=[tls_secret]
    )

    # When
    with patch.multiple(
        "managers.tls.TLSManager",
        set_private_key=DEFAULT,
        set_ca=DEFAULT,
        set_certificate=DEFAULT,
        set_truststore=DEFAULT,
        set_p12_keystore=DEFAULT,
    ):
        state_out = ctx.run(ctx.on.relation_changed(tls_relation), state_in)

    # Then
    secret_out = state_out.get_secret(label=f"{PEER}.zookeeper{secret_suffix}.unit")
    assert secret_out.latest_content is not None
    assert secret_out.latest_content.get("certificate", "") == "cert"
    assert secret_out.latest_content.get("ca-cert", "") == "ca"


def test_renew_certificates_auto_reload(ctx: Context, base_state: State) -> None:
    # Given
    provider_data = {
        "certificates": json.dumps(
            [
                {
                    "certificate_signing_request": "not-missing",
                    "ca": "ca",
                    "certificate": "new-cert",
                    "chain": ["ca", "cert"],
                }
            ]
        )
    }
    requirer_data = {
        "certificate_signing_requests": json.dumps(
            [{"certificate_signing_request": "not-missing"}]
        )
    }
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_app_data={"tls": "enabled", "switching-encryption": "started"},
        local_unit_data={"csr": "not-missing", "certificate": "old-cert", "ca-cert": "ca"},
        peers_data={},
    )
    tls_relation = Relation(
        CERTS_REL_NAME, TLS_NAME, remote_app_data=provider_data, local_unit_data=requirer_data
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with patch.multiple(
        "managers.tls.TLSManager",
        set_private_key=DEFAULT,
        set_ca=DEFAULT,
        set_certificate=DEFAULT,
        set_truststore=DEFAULT,
        set_p12_keystore=DEFAULT,
    ):
        state_out = ctx.run(ctx.on.relation_changed(tls_relation), state_in)

    # Then
    unit_data = state_out.get_relation(cluster_peer.id).local_unit_data
    assert unit_data.get("certificate", "") == "new-cert"
    assert unit_data.get("ca-cert", "") == "ca"


def test_certificates_available_halfway_through_upgrade_succeeds(
    ctx: Context, base_state: State
) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER)
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    tls_secret = Secret(
        {"csr": "not-missing"}, label=f"{PEER}.zookeeper{secret_suffix}.unit", owner="unit"
    )
    state_in = dataclasses.replace(
        base_state, relations=[cluster_peer, tls_relation], secrets=[tls_secret]
    )

    # When
    with (
        patch.multiple(
            "managers.tls.TLSManager",
            set_private_key=DEFAULT,
            set_ca=DEFAULT,
            set_certificate=DEFAULT,
            set_truststore=DEFAULT,
            set_p12_keystore=DEFAULT,
        ),
        ctx(ctx.on.relation_changed(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        charm.tls_events.certificates.on.certificate_available.emit(
            certificate_signing_request="not-missing",
            certificate="cert",
            ca="ca",
            chain=["ca", "cert"],
        )
        state_out = manager.run()

        # Then
        assert charm.state.unit_server.certificate
        assert charm.state.unit_server.ca_cert

        # The correct CSR is preserved still in databag
        assert charm.state.unit_server.csr == "not-missing"

    # The certs are saved in a secret, with expected keys
    secret = state_out.get_secret(label=f"{PEER}.zookeeper{secret_suffix}.unit")
    assert secret.latest_content is not None
    assert secret.latest_content.items() >= {"certificate": "cert", "ca-cert": "ca"}.items()


def test_certificates_broken(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(PEER, PEER)
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    tls_secret = Secret(
        {"csr": "not-missing", "certificate": "cert", "ca-cert": "exists"},
        label=f"{PEER}.zookeeper{secret_suffix}.unit",
        owner="unit",
    )
    state_in = dataclasses.replace(
        base_state, relations=[cluster_peer, tls_relation], secrets=[tls_secret]
    )

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert charm.state.unit_server.certificate
        assert charm.state.unit_server.ca_cert
        assert charm.state.unit_server.csr

    # When
    with (
        patch.multiple(
            "managers.tls.TLSManager",
            remove_stores=DEFAULT,
        ),
        ctx(ctx.on.relation_broken(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert not charm.state.unit_server.certificate
        assert not charm.state.unit_server.ca_cert
        assert not charm.state.unit_server.csr
        assert not charm.state.cluster.tls
        assert charm.state.cluster.switching_encryption


def test_certificates_broken_after_upgrade(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_app_data={"tls": "enabled"},
        local_unit_data={"csr": "not-missing", "certificate": "cert", "ca": "exists"},
        peers_data={},
    )
    tls_relation = Relation(CERTS_REL_NAME, TLS_NAME)
    state_in = dataclasses.replace(base_state, relations=[cluster_peer, tls_relation])

    # When
    with ctx(ctx.on.start(), state_in) as manager:
        charm = cast(ZooKeeperCharm, manager.charm)

        # Then
        assert charm.state.unit_server.certificate
        assert charm.state.unit_server.ca_cert
        assert charm.state.unit_server.csr
        assert charm.state.cluster.tls

    # When
    with (
        patch.multiple(
            "managers.tls.TLSManager",
            remove_stores=DEFAULT,
        ),
        ctx(ctx.on.relation_broken(tls_relation), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()
        assert not charm.state.unit_server.certificate
        assert not charm.state.unit_server.ca_cert
        assert not charm.state.unit_server.csr
        assert not charm.state.cluster.tls
        assert charm.state.cluster.switching_encryption

    return


def test_certificates_expiring(ctx: Context, base_state: State) -> None:
    # Given
    key = open("tests/keys/0.key").read()
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_unit_data={
            "csr": "csr",
            "private-key": key,
            "certificate": "cert",
            "hostname": "treebeard",
            "ip": "1.1.1.1",
            "fqdn": "fangorn",
        },
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer])
    key = open("tests/keys/0.key").read()

    # Given
    with (
        patch(
            "charms.tls_certificates_interface.v1.tls_certificates.TLSCertificatesRequiresV1.request_certificate_renewal",
            return_value=None,
        ),
        ctx(ctx.on.start(), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        charm.tls_events.certificates.on.certificate_expiring.emit(certificate="cert", expiry=None)

        # Then
        assert charm.state.unit_server.csr != "csr"


def test_set_tls_private_key(ctx: Context, base_state: State) -> None:
    # Given
    cluster_peer = PeerRelation(
        PEER,
        PEER,
        local_unit_data={
            "csr": "csr",
            "private-key": "mellon",
            "certificate": "cert",
            "hostname": "treebeard",
            "ip": "1.1.1.1",
            "fqdn": "fangorn",
        },
    )
    state_in = dataclasses.replace(base_state, relations=[cluster_peer])
    key = open("tests/keys/0.key").read()

    # When
    with (
        patch(
            "charms.tls_certificates_interface.v1.tls_certificates.TLSCertificatesRequiresV1.request_certificate_renewal",
            return_value=None,
        ),
        ctx(ctx.on.action("set-tls-private-key", {"internal-key": key}), state_in) as manager,
    ):
        charm = cast(ZooKeeperCharm, manager.charm)
        manager.run()

        # Then
        assert charm.state.unit_server.csr != "csr"
