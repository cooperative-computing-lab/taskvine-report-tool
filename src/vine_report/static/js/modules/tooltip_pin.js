let pinnedOwner = null;
let releaseHandlersBound = false;

function bindReleaseHandlers() {
    if (releaseHandlersBound) return;
    releaseHandlersBound = true;
    document.addEventListener('keydown', event => {
        if (event.key === 'Escape' && pinnedOwner) {
            pinnedOwner.releasePinnedTooltip?.();
        }
    });
    document.addEventListener('click', event => {
        if (!pinnedOwner) return;
        const target = event.target;
        if (target.closest?.('[data-vine-mark], .line-cursor-hitbox')) return;
        pinnedOwner.releasePinnedTooltip?.();
    });
}

export function pinTooltip(owner) {
    bindReleaseHandlers();
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
