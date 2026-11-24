export function metricContractDetails(contract = {}) {
    return [contract.status_reason, contract.caveat].filter(Boolean).join(' ');
}

export function metricContractError(contract = {}, fallback = 'The required source evidence is unavailable.') {
    return contract.status_reason || contract.caveat || fallback;
}
