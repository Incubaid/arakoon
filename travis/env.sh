OPAM_DIR=/tmp/opam
PATH=$OPAM_DIR/bin:$PATH

if test -d $OPAM_DIR/bin;
then
        eval `opam config -env`
fi
