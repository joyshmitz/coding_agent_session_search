/**
 * cass Archive Viewer - Main Application Module
 *
 * Ties together search, conversation viewer, and database modules.
 * Manages application state and view transitions.
 */

import { isDatabaseReady, getStatistics, closeDatabase } from './database.js';
import { initSearch, clearSearch } from './search.js';
import { initConversationViewer, loadConversation, clearViewer } from './conversation.js';

// Application state
const state = {
    view: 'search', // 'search' | 'conversation'
    conversationId: null,
    messageId: null,
};

// DOM element references
let elements = {
    appContent: null,
    searchView: null,
    conversationView: null,
    statsDisplay: null,
};

/**
 * Initialize the viewer application
 */
export function init() {
    console.log('[Viewer] Initializing...');

    // Get the app content container
    elements.appContent = document.getElementById('app-content');

    if (!elements.appContent) {
        console.error('[Viewer] App content container not found');
        return;
    }

    // Check if database is ready
    if (!isDatabaseReady()) {
        console.log('[Viewer] Waiting for database...');
        // Listen for database ready event
        window.addEventListener('cass:db-ready', handleDatabaseReady);
        return;
    }

    // Database is ready, initialize views
    initializeViews();
}

/**
 * Handle database ready event
 */
function handleDatabaseReady(event) {
    console.log('[Viewer] Database ready:', event.detail);
    window.removeEventListener('cass:db-ready', handleDatabaseReady);
    initializeViews();
}

/**
 * Initialize views after database is ready
 */
function initializeViews() {
    // Clear loading state
    elements.appContent.innerHTML = '';

    // Create view containers
    createViewContainers();

    // Initialize search view
    initSearch(elements.searchView, handleResultSelect);

    // Initialize conversation viewer
    initConversationViewer(elements.conversationView, handleBackToSearch);

    // Show search view by default
    showView('search');

    // Display stats
    displayStats();

    console.log('[Viewer] Initialized');
}

/**
 * Create view containers
 */
function createViewContainers() {
    elements.appContent.innerHTML = `
        <div id="stats-display" class="stats-display"></div>
        <div id="search-view" class="view-container"></div>
        <div id="conversation-view" class="view-container hidden"></div>
    `;

    elements.searchView = document.getElementById('search-view');
    elements.conversationView = document.getElementById('conversation-view');
    elements.statsDisplay = document.getElementById('stats-display');
}

/**
 * Display archive statistics
 */
function displayStats() {
    try {
        const stats = getStatistics();

        elements.statsDisplay.innerHTML = `
            <div class="stats-container">
                <div class="stat-item">
                    <span class="stat-value">${stats.conversations}</span>
                    <span class="stat-label">Conversations</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">${stats.messages}</span>
                    <span class="stat-label">Messages</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">${stats.agents.length}</span>
                    <span class="stat-label">Agents</span>
                </div>
            </div>
        `;
    } catch (error) {
        console.error('[Viewer] Failed to display stats:', error);
        elements.statsDisplay.innerHTML = '';
    }
}

/**
 * Show a specific view
 */
function showView(viewName) {
    state.view = viewName;

    if (viewName === 'search') {
        elements.searchView.classList.remove('hidden');
        elements.conversationView.classList.add('hidden');
        elements.statsDisplay.classList.remove('hidden');
    } else if (viewName === 'conversation') {
        elements.searchView.classList.add('hidden');
        elements.conversationView.classList.remove('hidden');
        elements.statsDisplay.classList.add('hidden');
    }

    // Update URL (without triggering navigation)
    updateUrl();
}

/**
 * Handle search result selection
 */
function handleResultSelect(conversationId, messageId = null) {
    state.conversationId = conversationId;
    state.messageId = messageId;

    // Load conversation
    loadConversation(conversationId, messageId);

    // Show conversation view
    showView('conversation');
}

/**
 * Handle back to search
 */
function handleBackToSearch() {
    clearViewer();
    state.conversationId = null;
    state.messageId = null;

    showView('search');
}

/**
 * Update URL without navigation
 */
function updateUrl() {
    const url = new URL(window.location.href);

    if (state.view === 'conversation' && state.conversationId) {
        url.searchParams.set('conv', state.conversationId);
        if (state.messageId) {
            url.searchParams.set('msg', state.messageId);
        } else {
            url.searchParams.delete('msg');
        }
    } else {
        url.searchParams.delete('conv');
        url.searchParams.delete('msg');
    }

    window.history.replaceState({}, '', url);
}

/**
 * Handle browser back/forward navigation
 */
function handlePopState() {
    const url = new URL(window.location.href);
    const convId = url.searchParams.get('conv');
    const msgId = url.searchParams.get('msg');

    if (convId) {
        handleResultSelect(parseInt(convId, 10), msgId ? parseInt(msgId, 10) : null);
    } else {
        handleBackToSearch();
    }
}

/**
 * Check URL on init for deep linking
 */
function checkDeepLink() {
    const url = new URL(window.location.href);
    const convId = url.searchParams.get('conv');
    const msgId = url.searchParams.get('msg');

    if (convId) {
        setTimeout(() => {
            handleResultSelect(parseInt(convId, 10), msgId ? parseInt(msgId, 10) : null);
        }, 100);
    }
}

/**
 * Clean up resources
 */
export function cleanup() {
    closeDatabase();
    clearSearch();
    clearViewer();
    console.log('[Viewer] Cleaned up');
}

/**
 * Get current application state
 */
export function getState() {
    return { ...state };
}

// Set up navigation handler
window.addEventListener('popstate', handlePopState);

// Check for deep links on page load
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', checkDeepLink);
} else {
    checkDeepLink();
}

// Export default
export default {
    init,
    cleanup,
    getState,
};
