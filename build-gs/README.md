# How to build rpm

An normal command is:

```
export MAKE_JFLAG=-j$your_processors
mkdir -p workdir
bash -xe ./percona-server-8.0_builder.sh --builddir=`pwd`/workdir --get_sources=1  --install_deps=1 --with_ssl=1 --build_tarball=0 --build_src_rpm=1  --build_rpm=1
```

And if u download an packaged source code of greatsql which does not contain any git info, u should use below step:

```
export MAKE_JFLAG=-j$your_processors
mkdir -p workdir
cp $your_path/greatsql-8.0.32-24.tar.gz workdir/
bash -xe ./percona-server-8.0_builder.sh --builddir=`pwd`/workdir --get_sources=0  --install_deps=1 --with_ssl=1 --build_tarball=0 --build_src_rpm=1  --build_rpm=1 --no_git_info=1
``` 

Other tips:
1. If already install all dep, then `install_deps` can set as 0. 
2. If already download boost, u can first copy to build-gs, and add paramter `--local_boost=1`

# How to build tarball

An normal command is:

```
export MAKE_JFLAG=-j$your_processors
export TAR_PROCESSORS=-T$your_processors
mkdir -p workdir
git submodule
git submodule sync --recursive
git submodule update --init --recursive
bash -xe ./percona-server-8.0_builder.sh --builddir=`pwd`/workdir --get_sources=0  --install_deps=1 --with_ssl=1 --build_tarball=1
```

And if u download an packaged source code of greatsql which does not contain any git info, u should use below step:

```
export TAR_PROCESSORS=-T$your_processors
export MAKE_JFLAG=-j$your_processors
mkdir -p workdir
bash -xe ./percona-server-8.0_builder.sh --builddir=`pwd`/workdir --get_sources=0  --install_deps=1 --with_ssl=1 --build_tarball=1 --no_git_info=1
```

The default package type is `tar.xz`.

# Other tips:

1. If already install all dep, then `install_deps` can set as 0. 
2. If already download boost, u can first copy to build-gs, and add paramter `--local_boost=1`
