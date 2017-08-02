export ASSERT_ON_STOMPING_PREVENTION=1

override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
override DFLAGS += -w

ifeq ($(DVER),1)
override DFLAGS += -v2 -v2=-static-arr-params -v2=-volatile
else
# Open source Makd uses dmd by default
DC = dmd-transitional
endif

$B/fakedls: $C/src/fakedls/main.d

all += $B/fakedls

$O/test-fakedls: $B/fakedls

run-fakedls: $O/test-fakedls $B/fakedls
	$(call exec, $O/test-fakedls $(TURTLE_ARGS))

debug-fakedls: $O/test-fakedls $B/fakedls
	$(call exec, gdb --args $O/test-fakedls $(TURTLE_ARGS))
