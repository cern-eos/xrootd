#!/usr/bin/env bash

######
# Starts a cluster configuration locally, instead of using
# containers.
#####

set -e

: ${XROOTD:=$(command -v xrootd)}
: ${CMSD:=$(command -v cmsd)}
: ${OPENSSL:=$(command -v openssl)}
: ${CRC32C:=$(command -v xrdcrc32c)}
: ${STAT:=$(command -v stat)}

servernames=("metaman" "man1" "man2" "srv1" "srv2" "srv3" "srv4")
datanodes=("srv1" "srv2" "srv3" "srv4")

DATAFOLDER="./data"
TMPDATAFOLDER="./rout"
PREDEF="./mvdata"
PORTS_ENV="./ports.env"

find_free_port() {
       python3 -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
}

allocate_ports() {
       local used=()
       local p

       find_unique_port() {
              while true; do
                     p=$(find_free_port)
                     for u in "${used[@]}"; do
                            [[ "$u" == "$p" ]] && continue 2
                     done
                     used+=("$p")
                     echo "$p"
                     return
              done
       }

       XRD_PORT_METAMAN=$(find_unique_port)
       CMSD_PORT_METAMAN=$(find_unique_port)
       XRD_PORT_MAN1=$(find_unique_port)
       CMSD_PORT_MAN1=$(find_unique_port)
       XRD_PORT_MAN2=$(find_unique_port)
       CMSD_PORT_MAN2=$(find_unique_port)
       XRD_PORT_SRV1=$(find_unique_port)
       XRD_PORT_SRV2=$(find_unique_port)
       XRD_PORT_SRV3=$(find_unique_port)
       XRD_PORT_SRV4=$(find_unique_port)

       export XRD_PORT_METAMAN CMSD_PORT_METAMAN
       export XRD_PORT_MAN1 CMSD_PORT_MAN1
       export XRD_PORT_MAN2 CMSD_PORT_MAN2
       export XRD_PORT_SRV1 XRD_PORT_SRV2 XRD_PORT_SRV3 XRD_PORT_SRV4
}

write_ports_env() {
       cat > "${PORTS_ENV}" <<EOF
XRD_PORT_METAMAN=${XRD_PORT_METAMAN}
CMSD_PORT_METAMAN=${CMSD_PORT_METAMAN}
XRD_PORT_MAN1=${XRD_PORT_MAN1}
CMSD_PORT_MAN1=${CMSD_PORT_MAN1}
XRD_PORT_MAN2=${XRD_PORT_MAN2}
CMSD_PORT_MAN2=${CMSD_PORT_MAN2}
XRD_PORT_SRV1=${XRD_PORT_SRV1}
XRD_PORT_SRV2=${XRD_PORT_SRV2}
XRD_PORT_SRV3=${XRD_PORT_SRV3}
XRD_PORT_SRV4=${XRD_PORT_SRV4}
HOST_METAMAN=root://localhost:${XRD_PORT_METAMAN}
HOST_MAN1=root://localhost:${XRD_PORT_MAN1}
HOST_MAN2=root://localhost:${XRD_PORT_MAN2}
HOST_SRV1=root://localhost:${XRD_PORT_SRV1}
HOST_SRV2=root://localhost:${XRD_PORT_SRV2}
HOST_SRV3=root://localhost:${XRD_PORT_SRV3}
HOST_SRV4=root://localhost:${XRD_PORT_SRV4}
EOF
}

apply_port_substitutions() {
       sed \
              -e "s/%XRD_PORT_METAMAN%/${XRD_PORT_METAMAN}/g" \
              -e "s/%CMSD_PORT_METAMAN%/${CMSD_PORT_METAMAN}/g" \
              -e "s/%XRD_PORT_MAN1%/${XRD_PORT_MAN1}/g" \
              -e "s/%CMSD_PORT_MAN1%/${CMSD_PORT_MAN1}/g" \
              -e "s/%XRD_PORT_MAN2%/${XRD_PORT_MAN2}/g" \
              -e "s/%CMSD_PORT_MAN2%/${CMSD_PORT_MAN2}/g" \
              -e "s/%XRD_PORT_SRV1%/${XRD_PORT_SRV1}/g" \
              -e "s/%XRD_PORT_SRV2%/${XRD_PORT_SRV2}/g" \
              -e "s/%XRD_PORT_SRV3%/${XRD_PORT_SRV3}/g" \
              -e "s/%XRD_PORT_SRV4%/${XRD_PORT_SRV4}/g"
}

write_configs() {
       local cfg

       for cfg in metaman man1 man2 srv1 srv2 srv3 srv4; do
              apply_port_substitutions < "${cfg}.cfg.template" > "${cfg}.cfg"
       done
}

patch_metalink_ports() {
       local file
       local sed_inplace

       case $(uname) in
       Darwin) sed_inplace=(-i '') ;;
       *)      sed_inplace=(-i) ;;
       esac

       for file in "$@"; do
              [[ -f "${file}" ]] || continue
              sed "${sed_inplace[@]}" \
                     -e "s/localhost:10943/localhost:${XRD_PORT_SRV1}/g" \
                     -e "s/localhost:10944/localhost:${XRD_PORT_SRV2}/g" \
                     -e "s/localhost:10945/localhost:${XRD_PORT_SRV3}/g" \
                     -e "s/localhost:10946/localhost:${XRD_PORT_SRV4}/g" \
                     "${file}"
       done
}

filenames=("1db882c8-8cd6-4df1-941f-ce669bad3458.dat"
       "3c9a9dd8-bc75-422c-b12c-f00604486cc1.dat"
       "7235b5d1-cede-4700-a8f9-596506b4cc38.dat"
       "7e480547-fe1a-4eaf-a210-0f3927751a43.dat"
       "89120cec-5244-444c-9313-703e4bee72de.dat"
       "a048e67f-4397-4bb8-85eb-8d7e40d90763.dat"
       "b3d40b3f-1d15-4ad3-8cb5-a7516acb2bab.dat"
       "b74d025e-06d6-43e8-91e1-a862feb03c84.dat"
       "cb4aacf1-6f28-42f2-b68a-90a73460f424.dat"
       "cef4d954-936f-4945-ae49-60ec715b986e.dat")

filesize() {
	case $(uname) in
	Darwin) ${STAT} -f"%z" $1 ;;
	Linux)  ${STAT} -c"%s" $1 ;;
	*)      ${STAT} -c"%s" $1 ;;
	esac
}

formatfiles() {
	case $(uname) in
              Darwin)
               sed -i '' "s|<size>.*</size>|<size>$new_size</size>|" $1
               sed -i '' "s|<hash type=\"crc32c\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               sed -i '' "s|<hash type=\"zcrc32\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               ;;
              Linux)
               sed -i "s|<size>.*</size>|<size>$new_size</size>|" $1
               sed -i "s|<hash type=\"crc32c\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               sed -i "s|<hash type=\"zcrc32\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               ;;
              *)
               sed -i "s|<size>.*</size>|<size>$new_size</size>|" $1
               sed -i "s|<hash type=\"crc32c\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               sed -i "s|<hash type=\"zcrc32\">.*</hash>|<hash type=\"crc32c\">$new_hash</hash>|" $1
               ;;
	esac
}

generate(){

       # check if files are in the data directory already...
       if [[ -e ${DATAFOLDER}/${i} ]]; then
              return
       fi

       mkdir -p ${TMPDATAFOLDER}

       for i in ${filenames[@]}; do
              ${OPENSSL} rand -out "${TMPDATAFOLDER}/${i}" $(( 2**24 ))
       done

       # correct the info inside of metalink files
       insertFileInfo

       # create local srv directories
       echo "Creating directories for each instance..."

       for i in ${servernames[@]}; do
              mkdir -p ${DATAFOLDER}/${i}/data
       done

       # create large file for reading in one request with max size readv
       ${OPENSSL} rand -out "${DATAFOLDER}/srv1/data/2GB.dat" $((2**31 - 1))

       for i in ${datanodes[@]}; do
              mkdir -p ${DATAFOLDER}/${i}/data/bigdir
              cd ${DATAFOLDER}/${i}/data/bigdir
              for i in `seq 1000`;
                     do touch `uuidgen`.dat;
              done
              cd - >/dev/null
       done

       for i in ${servernames[@]}; do
              if [[ ${i} == 'metaman' ]] ; then
                     # download the a test file for upload tests
                     mkdir -p ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/a048e67f-4397-4bb8-85eb-8d7e40d90763.dat ${DATAFOLDER}/${i}/data/testFile.dat
              fi

              # download the test files for 'srv1'
              if [[ ${i} == 'srv1' ]] ; then
                     cp ${TMPDATAFOLDER}/a048e67f-4397-4bb8-85eb-8d7e40d90763.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/b3d40b3f-1d15-4ad3-8cb5-a7516acb2bab.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/b74d025e-06d6-43e8-91e1-a862feb03c84.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/cb4aacf1-6f28-42f2-b68a-90a73460f424.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/cef4d954-936f-4945-ae49-60ec715b986e.dat ${DATAFOLDER}/${i}/data
                     mkdir -p ${DATAFOLDER}/${i}/data/metalink
                     cp ${PREDEF}/input*.meta* ${DATAFOLDER}/${i}/data/metalink/
                     cp ${PREDEF}/ml*.meta*    ${DATAFOLDER}/${i}/data/metalink/
                     patch_metalink_ports ${DATAFOLDER}/${i}/data/metalink/*
              fi

              # download the test files for 'srv2' and add another instance on 1099
              if [[ ${i} == 'srv2' ]] ; then
                     cp ${TMPDATAFOLDER}/1db882c8-8cd6-4df1-941f-ce669bad3458.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/3c9a9dd8-bc75-422c-b12c-f00604486cc1.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/7235b5d1-cede-4700-a8f9-596506b4cc38.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/7e480547-fe1a-4eaf-a210-0f3927751a43.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/89120cec-5244-444c-9313-703e4bee72de.dat ${DATAFOLDER}/${i}/data
              fi

              # download the test files for 'srv3'
              if [[ ${i} == 'srv3' ]] ; then
                     cp ${TMPDATAFOLDER}/1db882c8-8cd6-4df1-941f-ce669bad3458.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/3c9a9dd8-bc75-422c-b12c-f00604486cc1.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/89120cec-5244-444c-9313-703e4bee72de.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/b74d025e-06d6-43e8-91e1-a862feb03c84.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/cef4d954-936f-4945-ae49-60ec715b986e.dat ${DATAFOLDER}/${i}/data
              fi

              # download the test files for 'srv4'
              if [[ ${i} == 'srv4' ]] ; then
                     cp ${TMPDATAFOLDER}/1db882c8-8cd6-4df1-941f-ce669bad3458.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/7e480547-fe1a-4eaf-a210-0f3927751a43.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/89120cec-5244-444c-9313-703e4bee72de.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/b74d025e-06d6-43e8-91e1-a862feb03c84.dat ${DATAFOLDER}/${i}/data
                     cp ${TMPDATAFOLDER}/cef4d954-936f-4945-ae49-60ec715b986e.dat ${DATAFOLDER}/${i}/data
                     cp ${PREDEF}/data.zip                                 ${DATAFOLDER}/${i}/data
                     cp ${PREDEF}/large.zip                                ${DATAFOLDER}/${i}/data
              fi
       done

       rm -rf ${TMPDATAFOLDER}
}

start(){
       allocate_ports
       write_configs
       write_ports_env
       generate
       set -x
       # start for each component
       for i in "${servernames[@]}"; do
              ${XROOTD} -b -k fifo -n ${i} -l xrootd.log -s xrootd.pid -c ${i}.cfg
       done

       # start cmsd in the redirectors
       for i in "${servernames[@]}"; do
              ${CMSD} -b -k fifo -n ${i} -l cmsd.log -s cmsd.pid -c ${i}.cfg
       done

       sleep 1
}

stop() {
	for i in "${servernames[@]}"; do
		if [[ -d "${i}" ]]; then
			kill -s TERM $(cat ${i}/cmsd.pid)
			kill -s TERM $(cat ${i}/xrootd.pid)
			rm -rf "${i}"
		fi
	done
	rm -f "${PORTS_ENV}"
}

insertFileInfo() {
       # modifies metalink data
       for file in ${filenames[@]}; do
              for i in ${PREDEF}/*.meta*; do
                     # update size and hash
                     if grep -q $file $i; then
                            new_size=$(filesize ${TMPDATAFOLDER}/${file})
                            new_hash=$(${CRC32C} < ${TMPDATAFOLDER}/${file} | cut -d' '  -f1)
                            $(formatfiles $i)
                     fi
              done
       done
}

usage() {
       echo $0 start or stop
}

[[ $# == 0 ]] && usage && exit 0

CMD=$1
shift
[[ $(type -t ${CMD}) == "function" ]] || die "unknown command: ${CMD}"
$CMD $@

