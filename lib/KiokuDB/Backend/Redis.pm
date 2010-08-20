package KiokuDB::Backend::Redis; # ex:sw=4 et:
use Moose;

use AnyEvent::Redis;

our $VERSION = '0.03';

has host => (
    is => 'ro',
    isa => 'Str',
);

has port => (
    is => 'ro',
    isa => 'Int',
);

has prefix => (
    is => 'ro',
    isa => 'Str',
    default => "",
);

has 'redis' => (
    is => 'rw',
    isa => 'AnyEvent::Redis',
    lazy_build => 1
);

with qw(
    KiokuDB::Backend
    KiokuDB::Backend::Serialize::Delegate
);

use namespace::clean -except => 'meta';

sub new_from_dsn_params {
    my($self, %args) = @_;

    if(delete($args{server}) =~ /^(.*?)(?::(\d+))?$/) {
        $args{host} ||= $1;
        $args{port} ||= $2;
    }

    $self->new(%args);
}

sub BUILD { shift->redis }

sub _build_redis {
    my($self)  = @_;

    AnyEvent::Redis->new(host => $self->host, port => $self->port);
}

sub delete {
    my ($self, @ids_or_entries) = @_;

    my $redis = $self->redis;

    my @uids = map { ref($_) ? $_->id : $_ } @ids_or_entries;

    my $cv = AE::cv;
    foreach my $id ( @uids ) {
        $cv->begin;
        $redis->del($id, sub { $cv->end });
    }

    # TODO Error checking
    $cv->recv;

    return;
}

sub exists {
    my ($self, @ids) = @_;
    warn "Exists: @ids";

    my @exists;

    my $redis = $self->redis;

    my $cv = AE::cv;
    my $i = 0;
    foreach my $id (@ids) {
        $cv->begin;

        # Copy for closure
        my $count = $i;
        $redis->exists($self->{prefix} . $id->id, sub {
                $exists[$count] = $_[1];
                $cv->end;
            }
        );

        $i++;
    }

    # TODO Error checking
    $cv->recv;

    return @exists;
}

sub insert {
    my ($self, @entries) = @_;
    return unless @entries;

    my $redis = $self->redis;

    my(@new, @update);

    foreach my $entry ( @entries ) {
        if($entry->has_prev) {
            push @update, $entry;
        } else {
            push @new, $entry;
        }
    }

    my $cv = AE::cv;

    if(@new) {
        $cv->begin;
        $redis->msetnx(map(($self->{prefix} . $_->id => $self->serialize($_)), @new),
            sub { $cv->end });
    }

    if(@update) {
        $cv->begin;
        $redis->mset(map(($self->{prefix} . $_->id => $self->serialize($_)), @new),
            sub { $cv->end });
    }

    $cv->recv;
}

sub get {
    my ($self, @ids) = @_;
    warn "Get @ids";

    my @ret;
    $self->redis->mget(map { $self->{prefix} . $_ } @ids, sub {
            # TODO Error checking
            @ret = map $self->deserialize($_), @ids[1 .. $#ids]
        });

    return @ret;
}

1;

__END__

=head1 NAME

KiokuDB::Backend::Redis - Redis backend for KiokuDB

=head1 SYNOPSIS


    use KiokuDB::Backend::Redis;

    my $kiokudb = KiokuDB->connect('Redis:server=127.0.0.1;debug=1);
    ...

=head1 DESCRPTION

This is a KiokuDB backend for Redis, a self proclaimed data structures server.
It is rather embryonic, but passes the tests.  I expect to expand it as I
explore Redis and KiokuDB.

=head1 SEE ALSO

L<http://code.google.com/p/redis/>

=head1 AUTHOR

Cory G Watson, C<< <gphat at cpan.org> >>

=head1 COPYRIGHT & LICENSE

Copyright 2009 Cory G Watson.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
