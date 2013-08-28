export VIMSERVER=DEV

function pyutil_env {
    export PS1="PYUTIL \W$ "
    export PYTHONPATH=`find_up_dir env.sh ~/work/pyutil`
}
