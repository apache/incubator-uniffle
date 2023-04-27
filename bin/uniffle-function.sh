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
  if [[ -n "${UNIFFLE_SHELL_SCRIPT_DEBUG}" ]]; then
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

  UNIFFLE_OPTION_USAGE[${UNIFFLE_OPTION_USAGE}]="${option}@${text}"
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

function uniffle_generic_java_subcmd_handler
{
  uniffle_java_exec "${UNIFFLE_SUBCMD}" "${UNIFFLE_CLASSNAME}" "${UNIFFLE_SUBCMD_ARGS[@]}"
}
