use v5.20.0;
use strict;
use warnings;
use feature 'say';

use JMAP::Tester;
use JSON::MaybeXS;
use LWP::UserAgent;
use HTTP::Request;
sub env_required {
  my ($name) = @_;
  die "missing required environment variable: $name\n"
    unless defined $ENV{$name} && length $ENV{$name};
  return $ENV{$name};
}

my $BASE = env_required('LPE_JMAP_BASE_URL');
my $OWNER_EMAIL = env_required('LPE_JMAP_OWNER_EMAIL');
my $OWNER_PASSWORD = env_required('LPE_JMAP_OWNER_PASSWORD');
my $GRANTEE_EMAIL = env_required('LPE_JMAP_GRANTEE_EMAIL');
my $GRANTEE_PASSWORD = env_required('LPE_JMAP_GRANTEE_PASSWORD');

my $json = JSON::MaybeXS->new(utf8 => 1, canonical => 1);
my $ua = LWP::UserAgent->new(timeout => 30);
$ua->agent('LPE Milestone4 JMAP::Tester/0.1');

my @failures;
my @notes;
my @cleanup;

sub pass { say "ok - $_[0]" }
sub note { push @notes, $_[0]; say "note - $_[0]" }
sub fail { push @failures, $_[0]; say "not ok - $_[0]" }
sub assert_true { $_[0] ? pass($_[1]) : fail($_[1]) }

sub http_json {
  my ($method, $path, $body, $token) = @_;
  my $url = $path =~ m{^https?://} ? $path : "$BASE$path";
  my $req = HTTP::Request->new($method => $url);
  $req->header('Content-Type' => 'application/json') if defined $body;
  $req->header('Authorization' => "Bearer $token") if defined $token;
  $req->content($json->encode($body)) if defined $body;
  my $res = $ua->request($req);
  my $decoded = length($res->decoded_content // '') ? eval { $json->decode($res->decoded_content) } : undef;
  return ($res, $decoded);
}

sub absolute_session_urls {
  my ($session) = @_;
  for my $key (qw(apiUrl downloadUrl uploadUrl eventSourceUrl stateUrl websocketUrl)) {
    next unless defined $session->{$key};
    if ($session->{$key} =~ m{^/}) {
      note("session $key is relative; normalizing for JMAP::Tester continuation");
      $session->{$key} = "$BASE$session->{$key}";
    }
  }
  if (my $ws = $session->{capabilities}{'urn:ietf:params:jmap:websocket'}) {
    if (defined $ws->{url} && $ws->{url} =~ m{^/}) {
      note("websocket capability url is relative; normalizing for JMAP::Tester-adjacent checks");
      $ws->{url} = "$BASE$ws->{url}";
    }
  }
}

sub login_account {
  my ($email, $password) = @_;
  my ($login_res, $login) = http_json('POST', '/api/mail/auth/login', { email => $email, password => $password });
  die "login failed for $email: " . $login_res->status_line unless $login_res->code == 200;
  my $token = $login->{token};
  my ($session_res, $session) = http_json('GET', '/api/jmap/session', undef, $token);
  die "session failed for $email: " . $session_res->status_line unless $session_res->code == 200;
  absolute_session_urls($session);
  my $account_id = $session->{primaryAccounts}{'urn:ietf:params:jmap:core'};
  ($account_id) = keys %{ $session->{accounts} // {} } unless $account_id;
  die "no JMAP account for $email" unless $account_id;
  my $tester = JMAP::Tester->new({
    use_json_typist => 0,
    default_using => [
      'urn:ietf:params:jmap:core',
      'urn:ietf:params:jmap:mail',
      'urn:ietf:params:jmap:submission',
      'urn:ietf:params:jmap:blob',
      'urn:ietf:params:jmap:contacts',
      'urn:ietf:params:jmap:calendars',
      'urn:ietf:params:jmap:vacationresponse',
    ],
  });
  $tester->configure_from_client_session($session);
  $tester->_access_token($token);
  return { email => $email, token => $token, session => $session, account_id => $account_id, tester => $tester };
}

sub request_ok {
  my ($acct, $label, $calls) = @_;
  my $res = $acct->{tester}->request({ methodCalls => $calls });
  if (!$res->is_success) {
    fail("$label HTTP/JMAP request failed");
    return undef;
  }
  my $triples = $res->as_stripped_triples;
  pass("$label request returned HTTP 200");
  return $triples;
}

sub method_by_id {
  my ($triples, $id) = @_;
  for my $triple (@$triples) {
    return $triple if $triple->[2] eq $id;
  }
  return undef;
}

sub first_role_mailbox_id {
  my ($mailboxes, $role) = @_;
  for my $mb (@$mailboxes) {
    return $mb->{id} if defined($mb->{role}) && $mb->{role} eq $role;
  }
  return undef;
}

sub create_contact {
  my ($acct) = @_;
  my $client_id = 'm4-contact-' . time;
  my $triples = request_ok($acct, 'ContactCard/set create', [[
    'ContactCard/set', {
      accountId => $acct->{account_id},
      create => { $client_id => {
        name => { full => 'Milestone 4 Contact' },
        emails => { main => { address => 'm4-contact@example.invalid' } },
      }},
    }, 'contact-set'
  ]]);
  my $body = method_by_id($triples, 'contact-set')->[1];
  my $id = $body->{created}{$client_id}{id};
  assert_true($id, 'ContactCard/set created a contact');
  push @cleanup, sub { request_ok($acct, 'ContactCard/set cleanup', [[ 'ContactCard/set', { accountId => $acct->{account_id}, destroy => [$id] }, 'contact-clean' ]]) if $id };
  return ($id, $body->{oldState}, $body->{newState});
}

sub create_event {
  my ($acct) = @_;
  my $client_id = 'm4-event-' . time;
  my $triples = request_ok($acct, 'CalendarEvent/set create', [[
    'CalendarEvent/set', {
      accountId => $acct->{account_id},
      create => { $client_id => {
        title => 'Milestone 4 Event',
        start => '2026-05-06T10:00:00',
        duration => 'PT30M',
        locations => { main => { name => 'Interop Lab' } },
        description => 'temporary interoperability event',
      }},
    }, 'event-set'
  ]]);
  my $body = method_by_id($triples, 'event-set')->[1];
  my $id = $body->{created}{$client_id}{id};
  assert_true($id, 'CalendarEvent/set created an event');
  push @cleanup, sub { request_ok($acct, 'CalendarEvent/set cleanup', [[ 'CalendarEvent/set', { accountId => $acct->{account_id}, destroy => [$id] }, 'event-clean' ]]) if $id };
  return ($id, $body->{oldState}, $body->{newState});
}

sub create_draft_email {
  my ($acct, $drafts_id) = @_;
  my $client_id = 'm4-email-' . time;
  my $triples = request_ok($acct, 'Email/set draft create', [[
    'Email/set', {
      accountId => $acct->{account_id},
      create => { $client_id => {
        mailboxIds => { $drafts_id => JSON::MaybeXS::true },
        keywords => { '$draft' => JSON::MaybeXS::true, '$seen' => JSON::MaybeXS::true },
        from => [ { email => $acct->{email}, name => 'Milestone 4' } ],
        to => [ { email => $acct->{email}, name => 'Milestone 4' } ],
        subject => 'Milestone 4 temporary draft',
        textBody => 'Temporary JMAP::Tester draft',
      }},
    }, 'email-set'
  ]]);
  my $body = method_by_id($triples, 'email-set')->[1];
  my $id = $body->{created}{$client_id}{id};
  assert_true($id, 'Email/set created a canonical draft');
  push @cleanup, sub { request_ok($acct, 'Email/set draft cleanup', [[ 'Email/set', { accountId => $acct->{account_id}, destroy => [$id] }, 'email-clean' ]]) if $id };
  return ($id, $body->{oldState}, $body->{newState});
}

sub run_owner_tests {
  my ($acct) = @_;
  my $session = $acct->{session};
  assert_true($session->{capabilities}{'urn:ietf:params:jmap:core'}, 'Session advertises core capability');
  assert_true($session->{accounts}{$acct->{account_id}}, 'Session exposes primary account');
  for my $url_key (qw(apiUrl uploadUrl downloadUrl)) {
    assert_true($session->{$url_key} =~ m{^https?://}, "Session $url_key usable by JMAP::Tester");
  }

  my $triples = request_ok($acct, 'Big Three query/get batch', [
    [ 'Mailbox/query', { accountId => $acct->{account_id} }, 'mbq' ],
    [ 'Mailbox/get', { accountId => $acct->{account_id}, '#ids' => { resultOf => 'mbq', name => 'Mailbox/query', path => '/ids' } }, 'mbg' ],
    [ 'Email/query', { accountId => $acct->{account_id}, limit => 5 }, 'eq' ],
    [ 'Email/get', { accountId => $acct->{account_id}, '#ids' => { resultOf => 'eq', name => 'Email/query', path => '/ids' }, properties => ['id','subject','mailboxIds','keywords','preview','bodyValues'], fetchTextBodyValues => JSON::MaybeXS::true, maxBodyValueBytes => 64 }, 'eg' ],
    [ 'AddressBook/query', { accountId => $acct->{account_id} }, 'abq' ],
    [ 'ContactCard/query', { accountId => $acct->{account_id}, limit => 10 }, 'cq' ],
    [ 'Calendar/query', { accountId => $acct->{account_id} }, 'calq' ],
    [ 'CalendarEvent/query', { accountId => $acct->{account_id}, limit => 10 }, 'ceq' ],
  ]);
  return unless $triples;
  my $mbg = method_by_id($triples, 'mbg')->[1];
  assert_true(ref($mbg->{list}) eq 'ARRAY', 'Mailbox/get returns list array');
  my $drafts_id = first_role_mailbox_id($mbg->{list}, 'drafts');
  assert_true($drafts_id, 'Mailbox/get exposes Drafts role');
  assert_true(ref(method_by_id($triples, 'eg')->[1]{list}) eq 'ARRAY', 'Email/get returns list array');
  assert_true(ref(method_by_id($triples, 'cq')->[1]{ids}) eq 'ARRAY', 'ContactCard/query returns ids array');
  assert_true(ref(method_by_id($triples, 'ceq')->[1]{ids}) eq 'ARRAY', 'CalendarEvent/query returns ids array');

  my $upload_blob = '{"milestone":"four","temporary":true,"items":[1,2,3,4]}';
  my $upload = $acct->{tester}->upload({ accountId => $acct->{account_id}, type => 'application/json', blob => \$upload_blob });
  assert_true($upload->is_success, 'JMAP::Tester upload succeeds');
  if (!$upload->is_success) {
    note('upload failure: ' . $upload->http_response->status_line . ' ' . $upload->http_response->decoded_content);
  }
  if ($upload->is_success) {
    my $blob_id = $upload->blob_id;
    my $blob_triples = request_ok($acct, 'Blob/get uploaded blob', [[
      'Blob/get', { accountId => $acct->{account_id}, ids => [$blob_id], properties => ['id','type','size','digest'] }, 'blob-get'
    ]]);
    my $blob_body = method_by_id($blob_triples, 'blob-get')->[1];
    assert_true(@{ $blob_body->{list} } == 1, 'Blob/get returns uploaded blob');
    my $download = $acct->{tester}->download({ accountId => $acct->{account_id}, blobId => $blob_id, type => 'application/json', name => 'm4.json' }, { accept => 'application/json' });
    assert_true($download->is_success, 'JMAP::Tester download succeeds');
  }

  my $contact_before = request_ok($acct, 'ContactCard/query before create', [[
    'ContactCard/query', { accountId => $acct->{account_id}, limit => 20 }, 'contact-before'
  ]]);
  my $contact_query_state = method_by_id($contact_before, 'contact-before')->[1]{queryState};
  my ($contact_id, $contact_old, $contact_new) = create_contact($acct);
  if ($contact_id) {
    my $changes = request_ok($acct, 'ContactCard changes/queryChanges', [
      [ 'ContactCard/changes', { accountId => $acct->{account_id}, sinceState => $contact_old }, 'contact-changes' ],
      [ 'ContactCard/queryChanges', { accountId => $acct->{account_id}, sinceQueryState => $contact_query_state, maxChanges => 20 }, 'contact-qc' ],
    ]);
    assert_true(scalar grep({ $_ eq $contact_id } @{ method_by_id($changes, 'contact-changes')->[1]{created} // [] }), 'ContactCard/changes reports created contact');
    assert_true(scalar grep({ $_->{id} eq $contact_id } @{ method_by_id($changes, 'contact-qc')->[1]{added} // [] }), 'ContactCard/queryChanges reports added contact');
  }

  my $event_before = request_ok($acct, 'CalendarEvent/query before create', [[
    'CalendarEvent/query', { accountId => $acct->{account_id}, limit => 20 }, 'event-before'
  ]]);
  my $event_query_state = method_by_id($event_before, 'event-before')->[1]{queryState};
  my ($event_id, $event_old, $event_new) = create_event($acct);
  if ($event_id) {
    my $changes = request_ok($acct, 'CalendarEvent changes/queryChanges', [
      [ 'CalendarEvent/changes', { accountId => $acct->{account_id}, sinceState => $event_old }, 'event-changes' ],
      [ 'CalendarEvent/queryChanges', { accountId => $acct->{account_id}, sinceQueryState => $event_query_state, maxChanges => 20 }, 'event-qc' ],
    ]);
    assert_true(scalar grep({ $_ eq $event_id } @{ method_by_id($changes, 'event-changes')->[1]{created} // [] }), 'CalendarEvent/changes reports created event');
    assert_true(scalar grep({ $_->{id} eq $event_id } @{ method_by_id($changes, 'event-qc')->[1]{added} // [] }), 'CalendarEvent/queryChanges reports added event');
  }

  if ($drafts_id) {
    my $email_before = request_ok($acct, 'Email/query before create', [[
      'Email/query', { accountId => $acct->{account_id}, filter => { inMailbox => $drafts_id }, limit => 20 }, 'email-before'
    ]]);
    my $email_query_state = method_by_id($email_before, 'email-before')->[1]{queryState};
    my ($email_id, $email_old, $email_new) = create_draft_email($acct, $drafts_id);
    if ($email_id) {
      my $changes = request_ok($acct, 'Email changes/queryChanges', [
        [ 'Email/changes', { accountId => $acct->{account_id}, sinceState => $email_old }, 'email-changes' ],
        [ 'Email/queryChanges', { accountId => $acct->{account_id}, filter => { inMailbox => $drafts_id }, sinceQueryState => $email_query_state, maxChanges => 20 }, 'email-qc' ],
      ]);
      assert_true(scalar grep({ $_ eq $email_id } @{ method_by_id($changes, 'email-changes')->[1]{created} // [] }), 'Email/changes reports created draft');
      assert_true(scalar grep({ $_->{id} eq $email_id } @{ method_by_id($changes, 'email-qc')->[1]{added} // [] }), 'Email/queryChanges reports added draft');
    }
  }
}

sub run_delegated_tests {
  my ($owner, $grantee) = @_;
  my $old_session = $grantee->{session};
  my $old_accounts = scalar keys %{ $old_session->{accounts} // {} };
  my ($res1, $grant1) = http_json('PUT', '/api/mail/delegation/mailboxes', { granteeEmail => $grantee->{email}, mayWrite => JSON::MaybeXS::true }, $owner->{token});
  my ($res2, $grant2) = http_json('PUT', '/api/mail/delegation/sender', { granteeEmail => $grantee->{email}, senderRight => 'send_on_behalf' }, $owner->{token});
  my ($res3, $grant3) = http_json('PUT', '/api/mail/shares', { kind => 'contacts', granteeEmail => $grantee->{email}, mayRead => JSON::MaybeXS::true, mayWrite => JSON::MaybeXS::true, mayDelete => JSON::MaybeXS::true, mayShare => JSON::MaybeXS::false }, $owner->{token});
  my ($res4, $grant4) = http_json('PUT', '/api/mail/shares', { kind => 'calendar', granteeEmail => $grantee->{email}, mayRead => JSON::MaybeXS::true, mayWrite => JSON::MaybeXS::true, mayDelete => JSON::MaybeXS::true, mayShare => JSON::MaybeXS::false }, $owner->{token});
  assert_true($res1->code == 200 && $res2->code == 200 && $res3->code == 200 && $res4->code == 200, 'Account API creates canonical delegated/shared grants');
  push @cleanup, sub {
    http_json('DELETE', "/api/mail/delegation/sender/send_on_behalf/$grantee->{account_id}", undef, $owner->{token});
    http_json('DELETE', "/api/mail/delegation/mailboxes/$grantee->{account_id}", undef, $owner->{token});
    http_json('DELETE', "/api/mail/shares/contacts/$grantee->{account_id}", undef, $owner->{token});
    http_json('DELETE', "/api/mail/shares/calendar/$grantee->{account_id}", undef, $owner->{token});
  };

  my $fresh = login_account($grantee->{email}, $GRANTEE_PASSWORD);
  assert_true(scalar(keys %{ $fresh->{session}{accounts} }) > $old_accounts, 'Grantee session gains delegated mailbox account');
  assert_true($fresh->{session}{accounts}{ $owner->{account_id} }, 'Grantee session includes owner mailbox account');
  my $caps = $fresh->{session}{accounts}{ $owner->{account_id} }{accountCapabilities} // {};
  assert_true($caps->{'urn:ietf:params:jmap:mail'}, 'Delegated mailbox exposes Mail capability');
  assert_true($caps->{'urn:ietf:params:jmap:submission'}, 'Delegated mailbox exposes Submission capability');

  my $triples = request_ok($fresh, 'Delegated mailbox and shared collection batch', [
    [ 'Mailbox/get', { accountId => $owner->{account_id} }, 'd-mbg' ],
    [ 'Identity/get', { accountId => $owner->{account_id} }, 'd-idg' ],
    [ 'AddressBook/query', { accountId => $fresh->{account_id} }, 'd-abq' ],
    [ 'Calendar/query', { accountId => $fresh->{account_id} }, 'd-calq' ],
  ]);
  assert_true(@{ method_by_id($triples, 'd-mbg')->[1]{list} // [] } > 0, 'Delegated Mailbox/get returns owner mailboxes');
  assert_true(@{ method_by_id($triples, 'd-idg')->[1]{list} // [] } > 0, 'Delegated Identity/get returns sender identity');
  assert_true(@{ method_by_id($triples, 'd-abq')->[1]{ids} // [] } > 0, 'Shared AddressBook/query returns collection ids');
  assert_true(@{ method_by_id($triples, 'd-calq')->[1]{ids} // [] } > 0, 'Shared Calendar/query returns collection ids');
}

my $owner = eval { login_account($OWNER_EMAIL, $OWNER_PASSWORD) };
if ($@) { fail("owner login/session: $@"); exit 1; }
my $grantee = eval { login_account($GRANTEE_EMAIL, $GRANTEE_PASSWORD) };
if ($@) { fail("grantee login/session: $@"); exit 1; }

my $ok = eval {
  run_owner_tests($owner);
  run_delegated_tests($owner, $grantee);
  1;
};
fail("fatal test harness error: $@") unless $ok;

for my $cleanup (reverse @cleanup) {
  eval { $cleanup->(); 1 } or note("cleanup warning: $@");
}

say "SUMMARY failures=" . scalar(@failures) . " notes=" . scalar(@notes);
if (@failures) {
  say "FAILURES:";
  say " - $_" for @failures;
  exit 1;
}
exit 0;



