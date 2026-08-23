let pinnedOwner = null;
let escapeHandlerBound = false;

function bindEscapeHandler() {
    if (escapeHandlerBound) return;
    escapeHandlerBound = true;
    document.addEventListener('keydown', event => {
        if (event.key === 'Escape' && pinnedOwner) {
            pinnedOwner.releasePinnedTooltip?.();
        }
    });
}

export function pinTooltip(owner) {
    bindEscapeHandler();
    if (pinnedOwner && pinnedOwner !== owner) {
        pinnedOwner.releasePinnedTooltip?.();
    }
    pinnedOwner = owner;
}

export function unpinTooltip(owner) {
    if (pinnedOwner === owner) pinnedOwner = null;
}

export function hasPinnedTooltip() {
    return pinnedOwner !== null;
}

export function tooltipPinnedByOther(owner) {
    return pinnedOwner !== null && pinnedOwner !== owner;
}
