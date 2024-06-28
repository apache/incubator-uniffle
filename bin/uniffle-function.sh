#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set +x
declare -a UNIFFLE_SUBCMD_USAGE
declare -a UNIFFLE_OPTION_USAGE
declare -a UNIFFLE_SUBCMD_USAGE_TYPES

#---
# uniffle_array_contains: Check if an array has a given value.
# @param1 element
# @param2 array
# @returns 0 = yes; 1 = no
#---
function uniffle_array_contains
{
  declare element=$1
  shift
  declare val

  if [[ "$#" -eq 0 ]]; then
    return 1
  fi

  for val in "${@}"; do
    if [[ "${val}" == "${element}" ]]; then
      return 0
    fi
  done
  return 1
}

#---
# uniffle_add_array_param: Add the `appendstring` if `checkstring` is not present in the given array.
# @param1 envvar
# @param2 appendstring
#---
function uniffle_add_array_param
{
  declare arrname=$1
  declare add=$2

  declare arrref="${arrname}[@]"
  declare array=("${!arrref}")

  if ! uniffle_array_contains "${add}" "${array[@]}"; then
    eval ${arrname}=\(\"\${array[@]}\" \"${add}\" \)
    uniffle_debug "$1 accepted $2"
  else
    uniffle_debug "$1 declined $2"
  fi
}

#---
# uniffle_error: Print a message to stderr.
# @param1 string
#---
function uniffle_error
{
  echo "$*" 1>&2
}

#---
# uniffle_debug: Print a message to stderr if --debug is turned on.
# @param1 string
#---
function uniffle_debug
{
  if [[ "${UNIFFLE_SHELL_SCRIPT_DEBUG}" = true ]]; then
    echo "DEBUG: $*" 1>&2
  fi
}

#---
# uniffle_exit_with_usage: Print usage information and exit with the passed.
# @param1 exitcode
# @return This function will always exit.
#---
function uniffle_exit_with_usage
{
  local exitcode=$1
  if [[ -z $exitcode ]]; then
    exitcode=1
  fi

  if declare -F uniffle_usage >/dev/null ; then
    uniffle_usage
  else
    uniffle_error "Sorry, no help available."
  fi
  exit $exitcode
}

#---
# uniffle_sort_array: Sort an array (must not contain regexps) present in the given array.
# @param1 arrayvar
#---
function uniffle_sort_array
{
  declare arrname=$1
  declare arrref="${arrname}[@]"
  declare array=("${!arrref}")
  declare oifs

  declare globstatus
  declare -a sa

  globstatus=$(set -o | grep noglob | awk '{print $NF}')

  set -f
  oifs=${IFS}

  IFS=$'\n' sa=($(sort <<<"${array[*]}"))

  eval "${arrname}"=\(\"\${sa[@]}\"\)

  IFS=${oifs}
  if [[ "${globstatus}" = off ]]; then
    set +f
  fi
}

#---
# uniffle_generate_usage: generate standard usage output and optionally takes a class.
# @param1 execname
# @param2 true|false
# @param3 [text to use in place of SUBCOMMAND]
#---
function uniffle_generate_usage
{
  declare cmd=$1
  declare takesclass=$2
  declare subcmdtext=${3:-"SUBCOMMAND"}
  declare haveoptions
  declare optstring
  declare havesubs
  declare subcmdstring
  declare cmdtype

  cmd=${cmd##*/}

  if [[ -n "${UNIFFLE_OPTION_USAGE_COUNTER}"
        && "${UNIFFLE_OPTION_USAGE_COUNTER}" -gt 0 ]]; then
    haveoptions=true
    optstring=" [OPTIONS]"
  fi

  if [[ -n "${UNIFFLE_SUBCMD_USAGE_COUNTER}"
        && "${UNIFFLE_SUBCMD_USAGE_COUNTER}" -gt 0 ]]; then
    havesubs=true
    subcmdstring=" ${subcmdtext} [${subcmdtext} OPTIONS]"
  fi

  echo "Usage: ${cmd}${optstring}${subcmdstring}"
  if [[ ${takesclass} = true ]]; then
    echo " or    ${cmd}${optstring} CLASSNAME [CLASSNAME OPTIONS]"
    echo "  where CLASSNAME is a user-provided Java class"
  fi

  if [[ "${haveoptions}" = true ]]; then
    echo ""
    echo "  OPTIONS is none or any of:"
    echo ""

    uniffle_generic_columnprinter "" "${UNIFFLE_OPTION_USAGE[@]}"
  fi

  if [[ "${havesubs}" = true ]]; then
    echo ""
    echo "  ${subcmdtext} is one of:"
    echo ""

    if [[ "${#UNIFFLE_SUBCMD_USAGE_TYPES[@]}" -gt 0 ]]; then
      uniffle_sort_array UNIFFLE_SUBCMD_USAGE_TYPES
      for subtype in "${UNIFFLE_SUBCMD_USAGE_TYPES[@]}"; do
        cmdtype="$(tr '[:lower:]' '[:upper:]' <<< ${subtype:0:1})${subtype:1}"
        printf "\n    %s Commands:\n\n" "${cmdtype}"
        uniffle_generic_columnprinter "${subtype}" "${UNIFFLE_SUBCMD_USAGE[@]}"
      done
    else
      uniffle_generic_columnprinter "" "${UNIFFLE_SUBCMD_USAGE[@]}"
    fi
    echo ""
    echo "${subcmdtext} may print help when invoked w/o parameters or with -h."
  fi
}

#---
# uniffle_generic_columnprinter: Print a screen-size aware two-column output,
# if reqtype is not null, only print those requested.
# @param1 reqtype
# @param2 array
#---
function uniffle_generic_columnprinter
{
  declare reqtype=$1
  shift
  declare -a input=("$@")
  declare -i i=0
  declare -i counter=0
  declare line
  declare text
  declare option
  declare giventext
  declare -i maxoptsize
  declare -i foldsize
  declare -a tmpa
  declare numcols
  declare brup

  if [[ -n "${COLUMNS}" ]]; then
    numcols=${COLUMNS}
  else
    numcols=$(tput cols) 2>/dev/null
    COLUMNS=${numcols}
  fi

  if [[ -z "${numcols}"
     || ! "${numcols}" =~ ^[0-9]+$ ]]; then
     numcols=75
  else
     ((numcols=numcols-5))
  fi

  while read -r line; do
    tmpa[${counter}]=${line}
    ((counter=counter+1))
    IFS='@' read -ra brup <<< "${line}"
    option="${brup[0]}"
    if [[ ${#option} -gt ${maxoptsize} ]]; then
      maxoptsize=${#option}
    fi
  done < <(for text in "${input[@]}"; do
    echo "${text}"
  done | sort)

  i=0
    ((foldsize=numcols-maxoptsize))

  until [[ $i -eq ${#tmpa[@]} ]]; do
    IFS='@' read -ra brup <<< "${tmpa[$i]}"

    option="${brup[0]}"
    cmdtype="${brup[1]}"
    giventext="${brup[2]}"

    if [[ -n "${reqtype}" && "${cmdtype}" != "${reqtype}" ]]; then
      ((i=i+1))
      continue
    fi

    if [[ -z "${giventext}" ]]; then
      giventext=${cmdtype}
    fi

    while read -r line; do
      printf "%-${maxoptsize}s   %-s\n" "${option}" "${line}"
      option=" "
    done < <(echo "${giventext}"| fold -s -w ${foldsize})
    ((i=i+1))
  done
}

#---
# uniffle_add_subcommand: Add a subcommand to the usage output.
# @param1 subcommand
# @param2 subcommandtype
# @param3 subcommanddesc
#---
function uniffle_add_subcommand
{
  declare subcmd=$1
  declare subtype=$2
  declare text=$3

  uniffle_add_array_param UNIFFLE_SUBCMD_USAGE_TYPES "${subtype}"

  # done in this order so that sort works later
  UNIFFLE_SUBCMD_USAGE[${UNIFFLE_SUBCMD_USAGE_COUNTER}]="${subcmd}@${subtype}@${text}"
  ((UNIFFLE_SUBCMD_USAGE_COUNTER=UNIFFLE_SUBCMD_USAGE_COUNTER+1))
}

#---
# uniffle_add_option: Add an option to the usage output.
# @param1 subcommand
# @param2 subcommanddesc
#---
function uniffle_add_option
{
  local option=$1
  local text=$2

  UNIFFLE_OPTION_USAGE[${UNIFFLE_OPTION_USAGE_COUNTER}]="${option}@${text}"
  ((UNIFFLE_OPTION_USAGE_COUNTER=UNIFFLE_OPTION_USAGE_COUNTER+1))
}

#---
# uniffle_java_setup: Configure/verify ${JAVA_HOME}
# return may exit on failure conditions
#---
function uniffle_java_setup
{
  # Bail if we did not detect it
  if [[ -z "${JAVA_HOME}" ]]; then
    uniffle_error "ERROR: JAVA_HOME is not set and could not be found."
    exit 1
  fi

  if [[ ! -d "${JAVA_HOME}" ]]; then
    uniffle_error "ERROR: JAVA_HOME ${JAVA_HOME} does not exist."
    exit 1
  fi

  JAVA="${JAVA_HOME}/bin/java"

  if [[ ! -x "$JAVA" ]]; then
    uniffle_error "ERROR: $JAVA is not executable."
    exit 1
  fi
}

#---
# uniffle_java_exec: Execute the Java `class`, passing along any `options`.
# @param1 command
# @param2 class
#---
function uniffle_java_exec
{
  # run a java command.  this is used for
  # non-daemons

  local command=$1
  local class=$2
  shift 2

  export CLASSPATH
  exec "${JAVA}" "-Dproc_${command}" "${class}" "$@"
}

#---
# uniffle_validate_classname: Verify that a shell command was passed a valid class name.
# @param1 command
# @return 0 = success; 1 = failure w/user message;
#---
function uniffle_validate_classname
{
  local class=$1
  shift 1

  if [[ ! ${class} =~ \. ]]; then
    # assuming the arg is typo of command if it does not conatain ".".
    # class belonging to no package is not allowed as a result.
    uniffle_error "ERROR: ${class} is not COMMAND nor fully qualified CLASSNAME."
    return 1
  fi
  return 0
}

#---
# uniffle_basic_init: Initialize some basic environment variables for Uniffle.
#---
function uniffle_basic_init
{
  CLASSPATH=""
  uniffle_debug "Initialize CLASSPATH"

  USER=${USER:-$(id -nu)}
  UNIFFLE_IDENT_STRING=${UNIFFLE_IDENT_STRING:-$USER}
  UNIFFLE_LOG_DIR=${RSS_LOG_DIR:-"${RSS_HOME}/logs"}
  UNIFFLE_LOGFILE=${UNIFFLE_LOGFILE:-uniffle.log}
  UNIFFLE_STOP_TIMEOUT=${UNIFFLE_STOP_TIMEOUT:-60}
  UNIFFLE_PID_DIR=${RSS_PID_DIR:-"${RSS_HOME}"}
  UNIFFLE_LOG_CONF_FILE=${LOG_CONF_FILE:-"${RSS_CONF_DIR}/log4j2.xml"}

  uniffle_debug "USER: "$USER
  uniffle_debug "UNIFFLE_IDENT_STRING: "$UNIFFLE_IDENT_STRING
  uniffle_debug "UNIFFLE_LOG_DIR: "$UNIFFLE_LOG_DIR
  uniffle_debug "UNIFFLE_LOGFILE: "$UNIFFLE_LOGFILE
  uniffle_debug "UNIFFLE_STOP_TIMEOUT: "$UNIFFLE_STOP_TIMEOUT
}

#---
# uniffle_parse_args: Initialize some basic environment variables for Uniffle.
#---
function uniffle_parse_args
{
  UNIFFLE_DAEMON_MODE="default"
  UNIFFLE_PARSE_COUNTER=0
  while true; do
    uniffle_debug "uniffle_parse_args: processing $1"
    case $1 in
      --daemon)
        shift
        UNIFFLE_DAEMON_MODE=$1
        uniffle_debug "UNIFFLE_DAEMON_MODE:"$UNIFFLE_DAEMON_MODE
        shift
        ((UNIFFLE_PARSE_COUNTER=UNIFFLE_PARSE_COUNTER+2))
        if [[ -z "${UNIFFLE_DAEMON_MODE}" || \
          ! "${UNIFFLE_DAEMON_MODE}" =~ ^st(art|op|atus)$ ]]; then
          uniffle_error "ERROR: --daemon must be followed by either \"start\", \"stop\", or \"status\"."
          uniffle_exit_with_usage 1
        fi
      ;;
      *)
        break
      ;;
    esac
  done

  uniffle_debug "uniffle_parse: requesting the caller to skip ${UNIFFLE_PARSE_COUNTER}"
}

#---
# uniffle_add_classpath: add jars or path to the classpath.
#---
function uniffle_add_classpath
{
  if [[ $1 =~ ^.*\*$ ]]; then
    local mp
    mp=$(dirname "$1")
    if [[ ! -d "${mp}" ]]; then
      uniffle_debug "Rejected CLASSPATH: $1 (not a dir)"
      return 1
    fi
  elif [[ ! $1 =~ ^.*\*.*$ ]] && [[ ! -e "$1" ]]; then
    uniffle_debug "Rejected CLASSPATH: $1 (does not exist)"
    return 1
  fi
  if [[ -z "${CLASSPATH}" ]]; then
    CLASSPATH=$1
    uniffle_debug "Initial CLASSPATH=$1"
  elif [[ ":${CLASSPATH}:" != *":$1:"* ]]; then
    if [[ "$2" = "before" ]]; then
      CLASSPATH="$1:${CLASSPATH}"
      uniffle_debug "Prepend CLASSPATH: $1"
    else
      CLASSPATH+=:$1
      uniffle_debug "Append CLASSPATH: $1"
    fi
  else
    uniffle_debug "Dupe CLASSPATH: $1"
  fi
  return 0
}

#---
# uniffle_start_daemon: Start the daemon process.
#---
function uniffle_start_daemon
{
  local command=$1
  local class=$2
  local pidfile=$3
  shift 3

  uniffle_debug "Final CLASSPATH: ${CLASSPATH}"
  uniffle_debug "Final UNIFFLE_OPTS: ${UNIFFLE_OPTS}"
  uniffle_debug "Final JAVA_HOME: ${JAVA_HOME}"
  uniffle_debug "java: ${JAVA}"
  uniffle_debug "Class name: ${class}"
  uniffle_debug "Command line options: $*"

  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    uniffle_error "ERROR:  Cannot write ${command} pid ${pidfile}."
  fi

  export CLASSPATH
  exec "${JAVA}" "-Dproc_${command}" ${UNIFFLE_OPTS} "${class}" "$@"
}

#---
# uniffle_rotate_log: rotate uniffle log.
#---
function uniffle_rotate_log
{
  local log=$1;
  local num=${2:-5};

  if [[ -f "${log}" ]]; then
    while [[ ${num} -gt 1 ]]; do
      let prev=${num}-1
      if [[ -f "${log}.${prev}" ]]; then
        mv "${log}.${prev}" "${log}.${num}"
      fi
      num=${prev}
    done
    mv "${log}" "${log}.${num}"
  fi
}

#---
# uniffle_start_daemon_wrapper: start uniffle daemon wrapper.
#---
function uniffle_start_daemon_wrapper
{
  local daemonname=$1
  local class=$2
  local pidfile=$3
  local outfile=$4

  shift 4

  local counter

  uniffle_rotate_log "${outfile}"

  uniffle_start_daemon "${daemonname}" \
    "$class" \
    "${pidfile}" \
    "$@" >> "${outfile}" 2>&1 < /dev/null &

  (( counter=0 ))
  while [[ ! -f ${pidfile} && ${counter} -le 5 ]]; do
    sleep 1
    (( counter++ ))
  done

  echo $! > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    uniffle_error "ERROR:  Cannot write ${daemonname} pid ${pidfile}."
  fi

  ulimit -a >> "${outfile}" 2>&1

  if ! ps -p $! >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

#---
# uniffle_start_daemon: check daemon status.
# @param        pidfile
# @return  0, The uniffle process has started and running
#          1, The uniffle process has started but dead
#          3, The uniffle process not running
#---
function uniffle_status_daemon
{
  local pidfile=$1
  shift

  local pid
  local pspid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "${pidfile}")
    if pspid=$(ps -o args= -p"${pid}" 2>/dev/null); then
      if [[ ${pspid} =~ -Dproc_${daemonname} ]]; then
        return 0
      fi
    fi
    return 1
  fi
  return 3
}

#---
# uniffle_daemon_handler: handling daemon requests for uniffle.
# @param daemonmode [start|stop|status|default]
# @param daemonname [coordinator|shuffle-server]
# @param class The main function that needs to be started.
# @param daemonpidfile
# @param daemonoutfile
#---
function uniffle_daemon_handler
{
  set +e

  local daemonmode=$1
  local daemonname=$2
  local class=$3
  local daemon_pidfile=$4
  local daemon_outfile=$5
  shift 5

  case ${daemonmode} in
    status)
      uniffle_status_daemon "${daemon_pidfile}"

      if [[ $? == 0 ]]; then
        echo "${daemonname} is running as process $(cat "${daemon_pidfile}")."
      elif [[ $? == 1 ]]; then
        echo "${daemonname} is stopped."
      else
        uniffle_error "uniffle_status_daemon error"
      fi

      exit $?
    ;;

    stop)
      uniffle_stop_daemon "${daemonname}" "${daemon_pidfile}"
      if [[ $? == 0 ]]; then
        echo "${daemonname} stop success."
      fi
      exit $?
    ;;

    start|default)

      uniffle_verify_piddir
      uniffle_verify_logdir
      uniffle_status_daemon "${daemon_pidfile}"

      if [[ $? == 0 ]]; then
        uniffle_error "${daemonname} is running as process $(cat "${daemon_pidfile}").  Stop it first and ensure ${daemon_pidfile} file is empty before retry."
        exit 1
      else
        rm -f "${daemon_pidfile}" >/dev/null 2>&1
      fi

      if [[ "$daemonmode" = "default" ]]; then
        uniffle_start_daemon "${daemonname}" "${class}" "${daemon_pidfile}" "$@"
      else
        uniffle_start_daemon_wrapper "${daemonname}" "${class}" "${daemon_pidfile}" "${daemon_outfile}" "$@"
      fi

      if [[ $? == 0 ]]; then
        echo "${daemonname} start success, is running as process $(cat "${daemon_pidfile}")."
      fi
    ;;
  esac

  set -e
}

#---
# uniffle_start_daemon: Start the Uniffle service.
# @param command [coordinator|shuffle-server]
# @param class The main function that needs to be started.
# @param pidfile pid file.
#---
function uniffle_start_daemon
{
  local command=$1
  local class=$2
  local pidfile=$3
  shift 3

  uniffle_debug "Final CLASSPATH: ${CLASSPATH}"
  uniffle_debug "Final UNIFFLE_OPTS: ${UNIFFLE_OPTS}"
  uniffle_debug "Final JAVA_HOME: ${JAVA_HOME}"
  uniffle_debug "java: ${JAVA}"
  uniffle_debug "Class name: ${class}"
  uniffle_debug "Command line options: $*"

  echo $$ > "${pidfile}" 2>/dev/null
  if [[ $? -gt 0 ]]; then
    uniffle_error "ERROR:  Cannot write ${command} pid ${pidfile}."
  fi

  export CLASSPATH
  exec "${JAVA}" "-Dproc_${command}" ${UNIFFLE_OPTS} "${class}" --conf "$RSS_CONF_FILE" "$@"
}

#---
# uniffle_stop_daemon: Stop the Uniffle service.
# @param command [coordinator|shuffle-server]
# @param pidfile pid file.
#---
function uniffle_stop_daemon
{
  local cmd=$1
  local pidfile=$2
  shift 2

  local pid
  local cur_pid

  if [[ -f "${pidfile}" ]]; then
    pid=$(cat "$pidfile")

    kill "${pid}" >/dev/null 2>&1

    wait_process_to_die_or_timeout "${pid}" "${UNIFFLE_STOP_TIMEOUT}"

    if kill -0 "${pid}" > /dev/null 2>&1; then
      uniffle_error "WARNING: ${cmd} did not stop gracefully after ${UNIFFLE_STOP_TIMEOUT} seconds: Trying to kill with kill -9"
      kill -9 "${pid}" >/dev/null 2>&1
    fi
    wait_process_to_die_or_timeout "${pid}" "${UNIFFLE_STOP_TIMEOUT}"
    if ps -p "${pid}" > /dev/null 2>&1; then
      uniffle_error "ERROR: Unable to kill ${pid}"
    else
      cur_pid=$(cat "$pidfile")
      if [[ "${pid}" = "${cur_pid}" ]]; then
        rm -f "${pidfile}" >/dev/null 2>&1
      else
        uniffle_error "WARNING: pid has changed for ${cmd}, skip deleting pid file"
      fi
    fi
  fi
}

#---
# wait_process_to_die_or_timeout: Wait for the program to terminate or timeout.
# @param pid process pid.
# @param timeout timeout duration.
#---
function wait_process_to_die_or_timeout
{
  local pid=$1
  local timeout=$2

  timeout=$(printf "%.0f\n" "${timeout}")
  if [[ ${timeout} -lt 1  ]]; then
    timeout=1
  fi

  for (( i=0; i < "${timeout}"; i++ ))
  do
    if kill -0 "${pid}" > /dev/null 2>&1; then
      sleep 1
    else
      break
    fi
  done
}

#---
# uniffle_verify_piddir: verify pid folder.
#---
function uniffle_verify_piddir
{
  uniffle_debug "uniffle_verify_piddir: "${UNIFFLE_PID_DIR}
  if [[ -z "${UNIFFLE_PID_DIR}" ]]; then
    uniffle_error "No pid directory defined."
    exit 1
  fi
  uniffle_mkdir "${UNIFFLE_PID_DIR}"
  touch "${UNIFFLE_PID_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    uniffle_error "ERROR: Unable to write in ${UNIFFLE_PID_DIR}. Aborting."
    exit 1
  fi
  rm "${UNIFFLE_PID_DIR}/$$" >/dev/null 2>&1
}

#---
# uniffle_verify_piddir: verify log folder.
#---
function uniffle_verify_logdir
{
  uniffle_debug "uniffle_verify_logdir: "${UNIFFLE_LOG_DIR}
  if [[ -z "${UNIFFLE_LOG_DIR}" ]]; then
    uniffle_error "No Uniffle log directory defined."
    exit 1
  fi
  uniffle_mkdir "${UNIFFLE_LOG_DIR}"
  touch "${UNIFFLE_LOG_DIR}/$$" >/dev/null 2>&1
  if [[ $? -gt 0 ]]; then
    uniffle_error "ERROR: Unable to write in ${UNIFFLE_LOG_DIR}. Aborting."
    exit 1
  fi
  rm "${UNIFFLE_LOG_DIR}/$$" >/dev/null 2>&1
}

#---
# uniffle_mkdir: uniffle mkdir.
#---
function uniffle_mkdir
{
  local dir=$1

  if [[ ! -w "${dir}" ]] && [[ ! -d "${dir}" ]]; then
    uniffle_error "WARNING: ${dir} does not exist. Creating."
    if ! mkdir -p "${dir}"; then
      uniffle_error "ERROR: Unable to create ${dir}. Aborting."
      exit 1
    fi
  fi
}

#---
# uniffle_finalize_uniffle_opts: Finish configuring Uniffle specific system properties.
#---
function uniffle_finalize_uniffle_opts
{
  set +u
  uniffle_add_param UNIFFLE_OPTS log4j2.configurationFile "-Dlog4j2.configurationFile=file:${UNIFFLE_LOG_CONF_FILE}"
  uniffle_add_param UNIFFLE_OPTS uniffle.log.dir "-Duniffle.log.dir=${UNIFFLE_LOG_DIR}"
  uniffle_add_param UNIFFLE_OPTS uniffle.home.dir "-Duniffle.home.dir=${RSS_HOME}"
  uniffle_add_param UNIFFLE_OPTS uniffle.id.str "-Duniffle.id.str=${UNIFFLE_IDENT_STRING}"
  set -u
}

#---
# uniffle_add_param: Append command line arguments.
#---
function uniffle_add_param
{
  if [[ ! ${!1} =~ $2 ]] ; then
    eval "$1"="'${!1} $3'"
    if [[ ${!1:0:1} = ' ' ]]; then
      eval "$1"="'${!1# }'"
    fi
    uniffle_debug "$1 accepted $3"
  else
    uniffle_debug "$1 declined $3"
  fi
}

#---
# uniffle_generic_java_subcmd_handler: Execute java common commands.
#---
function uniffle_generic_java_subcmd_handler
{
  declare priv_outfile
  declare priv_errfile
  declare priv_pidfile
  declare daemon_outfile
  declare daemon_pidfile

  if [[ "${UNIFFLE_SUBCMD_SUPPORT_DAEMONIZATION}" = true ]]; then

    daemon_outfile="${UNIFFLE_LOG_DIR}/uniffle-${UNIFFLE_IDENT_STRING}-${UNIFFLE_SUBCMD}-${HOSTNAME}.out"
    daemon_pidfile="${UNIFFLE_PID_DIR}/uniffle-${UNIFFLE_IDENT_STRING}-${UNIFFLE_SUBCMD}.pid"
    UNIFFLE_LOGFILE="uniffle-${UNIFFLE_IDENT_STRING}-${UNIFFLE_SUBCMD}-${HOSTNAME}.log"
    uniffle_add_param UNIFFLE_OPTS log.path "-Dlog.path=${UNIFFLE_LOG_DIR}/${UNIFFLE_LOGFILE}"

    uniffle_debug "Daemon Outfile: ${daemon_outfile}"
    uniffle_debug "Daemon Pidfile: ${daemon_pidfile}"
    uniffle_debug "UNIFFLE_DAEMON_MODE: "$UNIFFLE_DAEMON_MODE
    uniffle_debug "UNIFFLE_SUBCMD: "$UNIFFLE_SUBCMD
    uniffle_debug "UNIFFLE_CLASSNAME: "$UNIFFLE_CLASSNAME
    uniffle_debug "UNIFFLE_SUBCMD_ARGS: "$UNIFFLE_SUBCMD_ARGS

    uniffle_daemon_handler \
        "${UNIFFLE_DAEMON_MODE}" \
        "${UNIFFLE_SUBCMD}" \
        "${UNIFFLE_CLASSNAME}" \
        "${daemon_pidfile}" \
        "${daemon_outfile}" \
        "${UNIFFLE_SUBCMD_ARGS[@]}"
    exit $?
  else
    uniffle_java_exec "${UNIFFLE_SUBCMD}" "${UNIFFLE_CLASSNAME}" "${UNIFFLE_SUBCMD_ARGS[@]}"
  fi
}