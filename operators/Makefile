
# This Makefile requires GNU make.

HOME = ../..

include $(HOME)/Defines.mk

LIBS += -L$(HOME)/$(LIB_DIR) -l$(PHOENIX)

OP_OBJS := main.o print_op.o utils.o select_op.o prepare_op.o project_op.o sort_op.o partition_op.o aggregate_op.o select_op_old.o


PROGS := main

.PHONY: default all clean

default: all

all: $(PROGS)

main: $(OP_OBJS) $(LIB_DEP)
	$(CC) $(CFLAGS) -g -ggdb -o $@ $(OP_OBJS) $(LIBS)

%.o: %.c *.h
	$(CC) $(CFLAGS) -c -g -ggdb $< -o $@ -I$(HOME)/$(INC_DIR)

clean:
	rm -f $(PROGS) $(OP_OBJS) 
