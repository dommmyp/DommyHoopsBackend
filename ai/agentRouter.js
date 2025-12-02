import dotenv from "dotenv";
dotenv.config();

import express from "express";
import OpenAI from "openai";
import { toolDefinitions, toolHandlers } from "./toolHandlers.js";

const router = express.Router();

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// ------------------------
// Helper to run tool calls
// ------------------------
async function runToolCall(toolCall) {
  const { name, arguments: argsJson } = toolCall.function || {};
  const handler = toolHandlers[name];

  if (!handler) {
    console.error("[AI agent] no handler for tool:", name);
    return {
      error: `No handler implemented for tool: ${name}`
    };
  }

  let args;
  try {
    args = argsJson ? JSON.parse(argsJson) : {};
  } catch (e) {
    console.error("[AI agent] failed to parse tool args", e, argsJson);
    return {
      error: "Failed to parse tool arguments JSON.",
      raw: String(argsJson || "")
    };
  }

  try {
    const result = await handler(args);
    return result;
  } catch (e) {
    console.error("[AI agent] tool error for", name, e);
    return { error: String(e) };
  }
}

// ------------------------
// POST /api/agent
// frontend sends: { messages: [...] }
// or: { question: "..." }
// ------------------------
router.post("/agent", async (req, res) => {
  const body = req.body || {};

  const clientMessages = Array.isArray(body.messages)
    ? body.messages.map(m => ({
        role: m.role,
        content: m.content
      }))
    : null;

  const question =
    typeof body.question === "string" ? body.question.trim() : "";

  if (!clientMessages && !question) {
    return res
      .status(400)
      .json({ error: "Missing 'question' or 'messages' in body." });
  }

  const userConversation =
    clientMessages ||
    [
      {
        role: "user",
        content: question
      }
    ];

  // Start conversation with system + whatever the frontend sent
  const conversationMessages = [
    {
      role: "system",
      content:
        "You are the DommyHoops basketball analytics assistant. " +
        "You must answer ONLY using data returned by the tools. " +
        "If the tools do not have enough information, say you do not have that data. " +
        "All data is NCAA Division I men's college basketball, 2025 season. " +
        "Be concise and specific, and show numbers when possible."
    },
    ...userConversation
  ];

  try {
    // allow multiple rounds of tools
    for (let step = 0; step < 4; step++) {
      const completion = await client.chat.completions.create({
        model: "gpt-4.1-mini",
        messages: conversationMessages,
        tools: toolDefinitions,
        tool_choice: "auto"
      });

      const message = completion.choices[0]?.message;

      // If the model wants to call tools
      if (message?.tool_calls && message.tool_calls.length > 0) {
        // 1. push the assistant message with tool_calls
        conversationMessages.push({
          role: "assistant",
          content: message.content || "",
          tool_calls: message.tool_calls
        });

        // 2. run tools and push tool messages after it
        for (const toolCall of message.tool_calls) {
          const result = await runToolCall(toolCall);
            console.log(result)

          conversationMessages.push({
            role: "tool",
            tool_call_id: toolCall.id,
            name: toolCall.function?.name || "unknown_tool",
            content: JSON.stringify(result)
          });
        }

        // go again so the model can see tool outputs
        continue;
      }

      // No tool calls: final answer
      const answerText = message?.content || "";

      // push final assistant message to conversation (optional)
      conversationMessages.push({
        role: "assistant",
        content: answerText
      });

      // match your frontend expectation: { message: { role, content } }
      return res.json({
        message: {
          role: "assistant",
          content: answerText
        }
      });
    }

    // If we exit loop without final answer
    return res
      .status(500)
      .json({ error: "Agent did not finish after multiple tool steps." });
  } catch (err) {
    console.error("[AI agent] top-level error", err);
    return res.status(500).json({
      error: "Error talking to AI agent.",
      details: String(err)
    });
  }
});

export default router;

