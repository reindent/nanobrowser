// API bridge for connecting nanobrowser to external systems
import { createLogger } from './log';

const logger = createLogger('api-bridge');

// WebSocket connection for external API communication
let apiSocket: WebSocket | null = null;
let apiReconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 5000; // 5 seconds

// Function to connect to the API bridge
export function connectToApiBridge() {
  try {
    // Connect to the local WebSocket server
    apiSocket = new WebSocket('ws://localhost:8787');

    apiSocket.onopen = () => {
      logger.info('Connected to nanobrowser API bridge');
      apiReconnectAttempts = 0;
    };

    apiSocket.onmessage = event => {
      try {
        const message = JSON.parse(event.data);
        logger.info('Received message from API bridge:', message);

        // Handle external task requests
        if (message.type === 'external_task') {
          handleExternalTask(message);
        }
      } catch (error) {
        logger.error('Error handling API bridge message:', error);
      }
    };

    apiSocket.onclose = () => {
      logger.info('Disconnected from nanobrowser API bridge');

      // Attempt to reconnect with backoff
      if (apiReconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        setTimeout(() => {
          apiReconnectAttempts++;
          logger.info(`Attempting to reconnect to API bridge (${apiReconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
          connectToApiBridge();
        }, RECONNECT_DELAY * apiReconnectAttempts);
      }
    };

    apiSocket.onerror = error => {
      logger.error('WebSocket error:', error);
    };
  } catch (error) {
    logger.error('Failed to connect to API bridge:', error);
  }
}

// Function to handle external task requests
async function handleExternalTask(message: any) {
  try {
    if (!message.task) {
      logger.error('External task missing required field: task');
      return;
    }

    // Get the active tab if tabId is not provided
    let tabId = message.tabId;
    if (!tabId) {
      const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
      if (tabs.length > 0 && tabs[0].id) {
        tabId = tabs[0].id;
      } else {
        logger.error('No active tab found for external task');
        return;
      }
    }

    logger.info('external_task', tabId, message.task);

    // Create a new executor for the task
    // We need to import these from the index to avoid circular dependencies
    const { setupExecutor, subscribeToExecutorEvents, browserContext } = await import('./index');

    const currentExecutor = await setupExecutor(message.taskId || `ext-${Date.now()}`, message.task, browserContext);

    // Subscribe to executor events
    subscribeToExecutorEvents(currentExecutor);

    // Execute the task
    const result = await currentExecutor.execute();
    logger.info('external_task execution result', tabId, result);

    // Send result back to API bridge if connected
    if (apiSocket && apiSocket.readyState === WebSocket.OPEN) {
      apiSocket.send(
        JSON.stringify({
          type: 'task_result',
          taskId: message.taskId,
          result: result,
        }),
      );
    }
  } catch (error) {
    logger.error('Error handling external task:', error);

    // Send error back to API bridge if connected
    if (apiSocket && apiSocket.readyState === WebSocket.OPEN) {
      apiSocket.send(
        JSON.stringify({
          type: 'task_error',
          taskId: message.taskId,
          error: error instanceof Error ? error.message : 'Unknown error',
        }),
      );
    }
  }
}
