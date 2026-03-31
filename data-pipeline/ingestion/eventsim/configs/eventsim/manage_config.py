# -*- coding: utf-8 -*-
"""
An example script converting the example-config into a config that simulates a scenario 
where there is a sharp increase in the frequency of the 'Error' page appearing.
"""

import json
from copy import deepcopy

def are_probas_valid(transitions):
    '''
    Validates that the transitions dictionary satisfies the probability constraint, i.e:

    For any source state s, the sum of P(s->d) over all possible destination states d must be <= 1
    '''
    check = {}
    for line in transitions:
        # a source state can be uniquely identified by (page, auth, level)
        src = line['source']
        src = (src['page'], src['auth'], src['level'])
        if src not in check:
            check[src] = 0
        check[src] += line['p']
    for src in check:
        if check[src] > 1:
            return False
    return True

def simulate_errors(config):
    """
    Converts the config into a new config that simulates a scenario where 
    there is a sharp increase in the frequency of the 'Error' page with code 408 appearing.

    Args:
        config - old config dict where transitions are clustered by their source state
    """
    STATUS_CODE = 408
    # Transition probabilities
    OTHERS_TO_ERROR = 0.9
    ERROR_TO_ERROR = 0.4
    ERROR_TO_END_SESSION = 0.5

    new_c = deepcopy(config)

    # Assign a high weight for the error page for new sessions
    for k in new_c['new-session']:
        if k['page'] == 'Error':
            k['status'] = STATUS_CODE
            k['weight'] = 50 if k['auth'] == 'Guest' else 2500

    # Modify the transitions
    prev = config['transitions'][0]['source']
    prev = (prev['page'], prev['auth'], prev['level'])
    accumulate_prev = []
    for i in range(len(config['transitions'])):
        current = config['transitions'][i]['source']
        current = (current['page'], current['auth'], current['level'])
        if current == prev:
            # Accumulate all transitions from the prev source state
            accumulate_prev.append(i)
        else:
            # New source state encountered, process all accumulated source states
            for j in accumulate_prev:
                line = new_c['transitions'][j]
                if prev[0] == 'Error':
                    if line['dest']['page'] == 'Error':
                        line['dest']['status'] = STATUS_CODE
                        line['p'] = ERROR_TO_ERROR
                    else:
                        line['p'] = (1-ERROR_TO_ERROR-ERROR_TO_END_SESSION)/(len(accumulate_prev))
                else:
                    if line['dest']['page'] == 'Error':
                        line['dest']['status'] = STATUS_CODE
                        line['p'] = OTHERS_TO_ERROR
                    else:
                        line['p'] = (1-OTHERS_TO_ERROR)/(len(accumulate_prev))
            prev = current

            accumulate_prev = [i]

    assert are_probas_valid(new_c['transitions']), "Transition probabilities not valid"

    return new_c

if __name__ == "__main__":
    with open("./example-config.json") as f:
        c = json.load(f)

    new_c = simulate_errors(c)

    with open('outage-config.json', 'w') as f:
        f.write(json.dumps(new_c, indent = 2))