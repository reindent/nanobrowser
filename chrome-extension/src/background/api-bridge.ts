// API bridge for connecting nanobrowser to external systems
import { createLogger } from './log';
import { ExecutionState } from './agent/event/types';
import type { Executor } from './agent/executor';

const NANOBROWSER_VERSION = '0.1.4';
const logger = createLogger('api-bridge');

// WebSocket connection for external API communication
let apiSocket: WebSocket | null = null;
let apiReconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 20;
const RECONNECT_DELAY = 3000; // 3 seconds

// Function to connect to the API bridge
export function connectToApiBridge() {
  try {
    // Connect to the local WebSocket server
    apiSocket = new WebSocket('ws://localhost:8787');

    apiSocket.onopen = () => {
      logger.info('Connected to nanobrowser API bridge');
      apiReconnectAttempts = 0;

      // Send hello message when connected
      if (apiSocket) {
        apiSocket.send(
          JSON.stringify({
            type: 'hello',
            client: 'nanobrowser-extension',
            version: NANOBROWSER_VERSION,
          }),
        );
      }
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
    const { setupExecutor, browserContext } = await import('./index');

    // Generate task ID if not provided
    const taskId = message.taskId || `ext-${Date.now()}`;

    const currentExecutor = await setupExecutor(taskId, message.task, browserContext);

    // Subscribe to executor events with our custom handler that streams to WebSocket
    subscribeToExecutorEventsWithStreaming(currentExecutor, taskId);

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
      logger.info(`Sent task result for task ${message.taskId}`);
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

// Function to subscribe to executor events and stream them to the WebSocket
function subscribeToExecutorEventsWithStreaming(executor: Executor, taskId: string) {
  logger.info(`Setting up event streaming for task: ${taskId}`);

  // Clear previous event listeners to prevent multiple subscriptions
  executor.clearExecutionEvents();

  // Subscribe to new events
  executor.subscribeExecutionEvents(async event => {
    logger.info(`Received executor event: ${event.actor}.${event.state} for task ${taskId}`);

    try {
      // Forward the event to the side panel if available
      // We need to use the subscribeToExecutorEvents function from index.ts
      // which already handles the side panel communication
      const { subscribeToExecutorEvents } = await import('./index');
      // We don't need to call it here as it's already being handled in the executor

      // Prepare the message for the WebSocket server
      const eventMessage = {
        type: 'agent_event',
        taskId: taskId,
        event: {
          actor: event.actor,
          state: event.state,
          timestamp: event.timestamp,
          data: {
            step: event.data.step,
            maxSteps: event.data.maxSteps,
            details: event.data.details,
          },
        },
      };

      // Send the message to the WebSocket server
      if (apiSocket && apiSocket.readyState === WebSocket.OPEN) {
        apiSocket.send(JSON.stringify(eventMessage));
        logger.info(`Streamed ${event.actor} event: ${event.state}`);
      } else {
        logger.error('WebSocket not connected, cannot stream event');
      }

      // Handle task completion
      if (
        event.state === ExecutionState.TASK_OK ||
        event.state === ExecutionState.TASK_FAIL ||
        event.state === ExecutionState.TASK_CANCEL
      ) {
        await executor.cleanup();
      }
    } catch (error) {
      logger.error('Failed to stream event:', error);
    }
  });
}
