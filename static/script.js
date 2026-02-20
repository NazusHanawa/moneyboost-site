function toggleFilters() {
    const container = document.getElementById('filters-collapsible');
    const btn = document.querySelector('.filter-toggle');
    container.classList.toggle('open');
    btn.classList.toggle('expanded');
}

document.addEventListener('DOMContentLoaded', function () {
    const searchInput = document.querySelector('input[name="q"]');
    const filterCheckboxes = document.querySelectorAll('input[name="platforms"]');
    const sortSelect = document.getElementById('sort-select');
    const cards = document.querySelectorAll('.store-card');
    const grid = document.querySelector('.grid');

    
    const storeData = Array.from(cards).map(card => {
        return {
            element: card,
            name: card.dataset.name,
            offers: JSON.parse(card.dataset.offers),
            hero: card.querySelector('.cashback-hero'),
            badgeName: card.querySelector('.badge-platform-name'),
            badgeContainer: card.querySelector('.platform-badge'),
            currentMax: 0
        };
    });

    function updateView() {
        const query = searchInput.value.toLowerCase().trim();
        const activePlatformIds = Array.from(filterCheckboxes)
            .filter(cb => cb.checked)
            .map(cb => parseInt(cb.value));

        const sortBy = sortSelect ? sortSelect.value : 'cashback-desc';
        const allFiltersDisabled = activePlatformIds.length === 0;

        const visibleStores = [];

        storeData.forEach(store => {
            
            if (!store.name.includes(query)) {
                store.element.style.display = 'none';
                return;
            }

            
            let displayValue = 0;
            let displayPlatform = '';
            let shouldShow = false;

            const viewModeRadio = document.querySelector('input[name="view-mode"]:checked');
            const viewMode = viewModeRadio ? viewModeRadio.value : 'global';

            if (allFiltersDisabled) {
                
                shouldShow = true;

                let maxVal = -1;
                let bestOffer = null;

                if (store.offers && store.offers.length > 0) {
                    store.offers.forEach(offer => {
                        let val = offer.value;
                        if (viewMode === 'max' && offer.value_specific > val) {
                            val = offer.value_specific;
                        }
                        if (val > maxVal) {
                            maxVal = val;
                            bestOffer = { ...offer, effectiveValue: val };
                        }
                    });
                }

                if (bestOffer) {
                    displayValue = bestOffer.effectiveValue;
                    displayPlatform = bestOffer.platform_name;
                } else {
                    displayValue = 0;
                    displayPlatform = '';
                }
            } else {
                
                let maxVal = -1;
                let bestOffer = null;

                
                const viewModeRadio = document.querySelector('input[name="view-mode"]:checked');
                const viewMode = viewModeRadio ? viewModeRadio.value : 'global';

                store.offers.forEach(offer => {
                    if (activePlatformIds.includes(parseInt(offer.platform_id))) {
                        
                        let val = offer.value;
                        if (viewMode === 'max' && offer.value_specific > val) {
                            val = offer.value_specific;
                        }

                        if (val > maxVal) {
                            maxVal = val;
                            bestOffer = { ...offer, effectiveValue: val };
                        }
                    }
                });

                if (bestOffer) {
                    shouldShow = true;
                    displayValue = bestOffer.effectiveValue;
                    displayPlatform = bestOffer.platform_name;
                } else {
                    
                    shouldShow = false;
                }
            }

            
            if (shouldShow) {
                store.hero.textContent = Number(displayValue.toFixed(2)) + '%';
                store.currentMax = displayValue;

                if (displayValue > 0 && displayPlatform) {
                    store.badgeName.textContent = displayPlatform;
                    store.badgeContainer.style.display = 'flex';
                } else {
                    store.badgeContainer.style.display = 'none';
                }

                store.element.style.display = 'flex';
                visibleStores.push(store);
            } else {
                store.element.style.display = 'none';
            }
        });

        
        visibleStores.sort((a, b) => {
            if (sortBy === 'cashback-desc') {
                return b.currentMax - a.currentMax;
            } else if (sortBy === 'name-asc') {
                return a.name.localeCompare(b.name);
            } else if (sortBy === 'name-desc') {
                return b.name.localeCompare(a.name);
            }
            return 0;
        });

        
        visibleStores.forEach(store => {
            grid.appendChild(store.element);
        });

        
        const countEl = document.getElementById('store-count');
        if (countEl) {
            const numEl = countEl.querySelector('.count-number');
            if (numEl) numEl.textContent = visibleStores.length;
        }
    }

    
    if (searchInput) {
        searchInput.addEventListener('input', updateView);
    }

    const viewModeRadios = document.querySelectorAll('input[name="view-mode"]');
    viewModeRadios.forEach(radio => {
        radio.addEventListener('change', updateView);
    });

    filterCheckboxes.forEach(cb => {
        cb.addEventListener('change', updateView);
    });

    if (sortSelect) {
        sortSelect.addEventListener('change', updateView);
    }

    const form = document.querySelector('.search-container');
    if (form) {
        form.addEventListener('submit', e => e.preventDefault());
    }

    
    updateView();
});
