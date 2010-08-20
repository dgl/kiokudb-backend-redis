use inc::Module::Install;

name     'KiokuDB-Backend-Redis';
all_from 'lib/KiokuDB/Backend/Redis.pm';
author   q{Cory G Watson <gphat@cpan.org>};
license  'perl';

build_requires 'Test::More';

requires 'AnyEvent::Redis' => '0.11';
requires 'KiokuDB' => '0.32';

WriteAll;

