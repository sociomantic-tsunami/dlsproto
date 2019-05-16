export ASSERT_ON_STOMPING_PREVENTION=1

override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
override DFLAGS += -w

override DFLAGS += -de
# Open source Makd uses dmd by default
DC = dmd-transitional

$B/fakedls: $C/src/fakedls/main.d

all += $B/fakedls $B/neotest

$O/test-fakedls: $B/fakedls

run-fakedls: $O/test-fakedls $B/fakedls
	$(call exec, $O/test-fakedls $(TURTLE_ARGS))

debug-fakedls: $O/test-fakedls $B/fakedls
	$(call exec, gdb --args $O/test-fakedls $(TURTLE_ARGS))

$B/neotest: override LDFLAGS += -lebtree -llzo2 -lrt -lgcrypt -lglib-2.0 -lgpg-error
$B/neotest: neotest/main.d
neotest: $B/neotest
