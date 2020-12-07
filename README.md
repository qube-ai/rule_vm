# Virtual Machine
This virutal machine is intented to execute rules for the Podnet. 
The customer creates rules for the devices to behave in a certain 
way on the frontend. That rule is taken by the rule VM and executed
to execute whatever action has to be executed.

## Caution
  - No rule should be named **immediate** in Firestore.

## Goal
The aim of this system is to provide instructions that are mapped to real world
as much as possible.


## Running

### Tests
```
$ pytest
```

### Run it on Compute Engine
Clone this repo, create a virtual env, install all the requirements there. And execute:
```
$ python gcp_interface.py
```


## Diagram
This image shows the overall system and what it does.
![](images/rule-mv-arch.jpg)

## How?

### How does rule gets parsed?
  - User selects the desired condition on the React frontend
  - We generate a JSON representation of the conditions and actions
  - The JSON representation is then saved to firestore
  - The Rule VM picks up the JSON representation of the rule when loading other rules from Firestore.
  - JSON data is parsed to create pythonic objects (Rule, Instruction and Action object)
  - VM then goes ahead and executes the rules that have to be executed.


## Instruction Set and JSON representation

### List of instructions
```
LOGICAL_AND
LOGICAL_OR

AT_TIME
AT_TIME_WITH_OCCURENCE
ENERGY_METER
DW_STATE
DW_STATE_FOR
OCCUPANCY
OCCUPANCY_FOR
TEMPERATURE
TEMPERATURE_FOR
RELAY_STATE
RELAY_STATE_FOR
```

### Logical Operation
#### Logical AND
```
<condition 1> AND <condition 2>
```

```json
[
  {"operation":  "some_condition_1"},
  {
  "operation": "logical_and"
  },
  {"operation":  "some_condition_2"},
]
```


#### Logical OR
```
<condition 1> OR <condition 2>
```

```json
[
  {"operation":  "some_condition_1"},
  {
  "operation": "logical_or"
  },
  {"operation":  "some_condition_2"},
]
```


### Time

#### AT_TIME
Is it past 6PM?
```
AT_TIME <time in RFC3339 format> 

AT_TIME 18:00:00+05:30
```

```json
[
  {
    "operation": "at_time",
    "time": "<time in RFC3339 format>"
  },
  
  // Other dictionaries

]
```

#### AT_TIME_WITH_OCCURRENCE

Turn off the balcony lights at 6PM everyday for 10 times.
```json
{
  "operation": "at_time_with_occurrence",
  "time": "<time in RFC3339 format>",
  "occurrence": 10
}
```

This will become true only when we are past the given time and for the given number of occurrences.
```
AT_TIME_WITH_OCCURRENCE <time in RFC3339 format> <no. of times, occurrence>

AT_TIME_WITH_OCCURRENCE 18:00:00+05:30 10
```

### ENERGY_METER
```
ENERGY_METER <device_id> <VARIABLE> <comparison_op> <value: float>

ENERGY_METER <device_id> VOLTAGE <comparison_op> <value>
ENERGY_METER <device_id> CURRENT <comparison_op> <value>
ENERGY_METER <device_id> REAL_POWER <comparison_op> <value>
ENERGY_METER <device_id> APPARENT_POWER <comparison_op> <value>
ENERGY_METER <device_id> POWER_FACTOR <comparison_op> <value: [0.0 to 1.0]>
ENERGY_METER <device_id> FREQUENCY <comparison_op> <value>

ENERGY_METER meter-1 power > 120
```

Possible values for **comparison** is **<, >, =**.

JSON representation
```json
{
    "operation":"energy_meter",
    "device_id": "meter-1",
    "variable": "voltage",
    "comparison_op": ">",
    "value": 220.0
}
```
If energy meter meter-1 voltage is more than 220, return true.


### DW_STATE
Checks if the door window state is of the given state

```
DW_STATE <device_id> <state: [OPEN | CLOSE]>

DW_STATE dw-1 OPEN
```

Is dw1 open?
```json
{
    "operation": "dw_state",
    "device_id": "dw-1",
    "state": "open"
}
```

#### DW_STATE_FOR
Checks if the door window state is open/close for given amount of time.

```
DW_STATE_FOR <device_id> <state: [OPEN | CLOSE]> <time in minutes>

DW_STATE_FOR dw-1 OPEN 20
```
This is will check whether dw-1 has been open for more than or equal to 20 minutes.

```json
{
    "operation": "dw_state_for",
    "device_id": "dw-1",
    "state": "open",
    "for": 20
}
```


### Occupancy Sensor

#### OCCUPANCY
JSON representation:
```
OCCUPANCY <device_id> <state: [OCCUPIED | UNOCCUPIED]>

OCCUPANCY_STATE os-1 OCCUPIED
```

```json
{
    "operation": "check_occupancy",
    "device_id": "occupancy-1",
    "state": "occupied"
}
```

#### OCCUPANCY_FOR

```
OCCUPANCY_STATE_FOR <device_id> <state: [OCCUPIED | UNOCCUPIED] <time in miunutes>

OCCUPANCY_STATE_FOR os-1 OCCUPIED 20
```

```json
{
    "operation": "occupancy_for",
    "device_id": "occupancy-1",
    "state": "occupied",
    "for": 21   // in minutes
}
```

### Relay States

#### RELAY_STATE
```json
{
    "operation": "relay_state",
    "device_id": "podnet-switch-1",
    "relay_index": 0,
    "state": 1
}
```

```
RELAY_STATE <device_id> <relay_index: starts from 0> <state:[0 | 1]

RELAY_STATE podnet-switch-1 0 1
```
Checks if relay index 0 of podnet-switch-1 is on.

#### RELAY_STATE_FOR
```
RELAY_STATE_FOR <device_id> <relay_index> <state:[on | off]> <time in minutes>

RELAY_STATE_FOR ps-1 0 1 15
```
Checks if the state of relay index 0 of device ps-1 is switched on for more than or equal to 15 minutes.


If relay0 of podnet-switch-1 on for more than/less than 10 minutes?
```json
{
    "operation": "relay_state_for",
    "device_id": "podnet-switch-1",
    "relay_index": 0,
    "state": 1,
    "for": 10   // in minutes
}
```


## Actions

Send an email
```json
{
    "type": "send_email",
    "to": ["apoorva.singh157@gmail.com", "duck@duck.com"],
    "subject": "Test Mail",
    "body": "You energy consumption has surpassed 150 units. To avoid getting charged extra. Use your electricity carefully."
}
```

(NOT IMPLEMENTED)
Change the state of a relay
```json
{
    "type": "change_relay_state",
    "device_id": "podnet-switch-1",
    "relay_index": 0,
    "state": 1
}
```


### Temperature Sensor (NOT USED)
Generic condition: `<Temperature, Device ID, Comparison OP, Value>`


If temperature is more than 31 degree Celsius.
```json 
{
    "operation": "check_temperature",
    "device_id": "podnet-switch-1",
    "comparison_op": "more than",
    "value": 31
}
```

```
TEMPERATURE <device_id> <comparison_op> <value>

TEMPERATURE ps-1 > 30
```
If the temperature of the given device is more than, less than, equal to the given temperature, return true.



If the temperature is 30 degree for more than 15 minutes, turn on the heater.
```json
{
    "operation": "check_temperature_for",
    "device_id": "podnet-switch-1",
    "comparison_op": "equal",
    "value": 30,
    "for": 15
}
```

```
TEMPERATURE_FOR <device_id> <comparison_op> <value> <time in minutes>

TEMPERATURE_FOR ps-1 > 30 15
```
If the temperature of ps-1 is more than 30 for more than or equal to 15 minutes.

