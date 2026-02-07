# This file contains the fixed structure migration code
# The key changes are:
# 1. Structure migration now runs as a background task (prevents "Failed to fetch")
# 2. AI translation uses parallel processing with batch_size=5 (faster, prevents timeout)
# 3. Progress updates work properly for real-time UI feedback

# Add this async function before line 1712 in backend/main.py:

async def run_structure_migration_task():
    """
    Background task for structure migration.
    Runs AI translation in parallel and then creates objects in target database.
    This prevents "Failed to fetch" errors by not blocking the request handler.
    """
    global structure_migration_progress
    migration_state["structure_running"] = True
    migration_state["structure_done"] = False
    
    async def _set_progress(percent: int, phase: str):
        global structure_migration_progress
        structure_migration_progress = {"percent": int(percent), "phase": phase}
        await asyncio.sleep(0)
    
    try:
        await _set_progress(0, "Initializing")
        
        session = await SessionModel.get_session()
        source = await ConnectionModel.get_by_id(session["source_id"])
        target = await ConnectionModel.get_by_id(session["target_id"])
        
        if not extraction_state.get("done") or not extraction_state.get("results"):
            migration_state["structure_running"] = False
            migration_state["structure_done"] = False
            print("[MIGRATION] Structure migration failed: Extraction not completed")
            return
        
        ddl_scripts = extraction_state["results"].get("ddl_scripts", {})
        
        tables_ddl = ddl_scripts.get("tables", [])
        views_ddl = ddl_scripts.get("views", [])
        sequences_ddl = ddl_scripts.get("sequences", [])
        
        all_ddl_objects = []
        for seq in sequences_ddl:
            all_ddl_objects.append({
                "name": seq.get("name", "unknown"),
                "schema": seq.get("schema", "public"),
                "kind": "sequence",
                "source_ddl": seq.get("ddl", "")
            })
        for table in tables_ddl:
            all_ddl_objects.append({
                "name": table.get("name", "unknown"),
                "schema": table.get("schema", "public"),
                "kind": "table",
                "source_ddl": table.get("ddl", "")
            })
        for view in views_ddl:
            all_ddl_objects.append({
                "name": view.get("name", "unknown"),
                "schema": view.get("schema", "public"),
                "kind": "view",
                "source_ddl": view.get("ddl", "")
            })
        
        total_objects = len(all_ddl_objects)
        
        await _set_progress(2, "Initializing")
        await _set_progress(5, "Starting AI translation")
        
        print(f"[MIGRATION] Starting AI translation from {source['db_type']} to {target['db_type']}")
        print(f"[MIGRATION] Translating {len(all_ddl_objects)} objects with parallel processing")
        
        # Import ai module
        ai = _import_ai_module()
        translated_objects: List[Dict[str, Any]] = []
        
        if total_objects == 0:
            translation = {"objects": []}
        else:
            # Process in batches for better performance with parallel AI calls
            batch_size = 5
            for batch_start in range(0, total_objects, batch_size):
                batch_end = min(batch_start + batch_size, total_objects)
                batch = all_ddl_objects[batch_start:batch_end]
                
                # Process batch in parallel using asyncio.gather
                tasks = []
                for obj in batch:
                    task = ai.translate_schema(
                        source["db_type"],
                        target["db_type"],
                        {"objects": [obj]}
                    )
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for i, result in enumerate(results):
                    obj = batch[i]
                    if isinstance(result, Exception):
                        print(f"[MIGRATION] AI translation error for {obj.get('name')}: {result}")
                        result = ai.fallback_translation([obj], source["db_type"], target["db_type"])
                    
                    if not isinstance(result, dict) or not result.get("objects"):
                        result = ai.fallback_translation([obj], source["db_type"], target["db_type"])
                    
                    translated_obj = (result.get("objects") or [{}])[0]
                    if translated_obj:
                        translated_objects.append(translated_obj)
                
                # Update progress after each batch
                progress = 10 + int((batch_end / total_objects) * 20)
                await _set_progress(progress, f"Translating objects ({batch_end}/{total_objects})")
            
            translation = {"objects": translated_objects}

        print(f"[MIGRATION] AI translation result: {len(translation.get('objects', []))} objects translated")

        await _set_progress(20, "AI translation complete")
        await _set_progress(25, "Processing translation results")
        await _set_progress(30, "Translation processing done")

        # Ensure source DDL is attached to each translated object for UI display.
        source_by_kind_name: Dict[tuple, str] = {}
        source_by_kind: Dict[tuple, str] = {}
        for obj in all_ddl_objects:
            source_by_kind_name[(obj.get("kind"), obj.get("schema"), obj.get("name"))] = obj.get("source_ddl", "")
            source_by_kind[(obj.get("kind"), obj.get("name"))] = obj.get("source_ddl", "")

        for obj in translation.get("objects", []):
            if obj.get("source_ddl"):
                continue
            key = (obj.get("kind"), obj.get("schema"), obj.get("name"))
            source_ddl = source_by_kind_name.get(key) or source_by_kind.get((obj.get("kind"), obj.get("name")))
            if not source_ddl and obj.get("name") and "." in str(obj.get("name")):
                name_only = str(obj.get("name")).split(".")[-1]
                source_ddl = source_by_kind.get((obj.get("kind"), name_only))
            if source_ddl:
                obj["source_ddl"] = source_ddl
        
        await _set_progress(40, "Preparing target database")
        await _set_progress(45, "Setting up target adapter")
        await _set_progress(50, "Preparing to create objects")
        
        target_creds = decrypt_credentials(target["enc_credentials"])
        target_adapter = get_adapter(target["db_type"], target_creds)
        await RunModel.update_status(session["run_id"], "structure_in_progress", mark_structure_start=True)
        
        await _set_progress(55, "Connecting to target database")
        await _set_progress(60, "Creating objects in target database")
        
        create_result = await target_adapter.create_objects(translation.get("objects", []))

        await _set_progress(80, "Objects created successfully")
        await _set_progress(90, "Updating run status")
        await _set_progress(95, "Finalizing structure migration")

        # Treat DDL failures as migration failures
        if not create_result.get("ok", True) or (create_result.get("errors")):
            migration_state["structure_done"] = False
            migration_state["results"] = {
                "translation": translation,
                "creation": create_result
            }
            error_msg = create_result.get("message") or "Structure migration encountered errors"
            errors = create_result.get("errors") or []
            first_error = errors[0].get("error") if errors and isinstance(errors[0], dict) else None
            if first_error:
                error_msg = f"{error_msg}. First error: {first_error}"
            print(f"[MIGRATION] Structure creation failed: {errors}")
            logger.error(f"[MIGRATION] Structure creation failed: {errors}")
            await RunModel.update_status(session["run_id"], "failed_structure", mark_complete=True)
            migration_state["structure_running"] = False
            structure_migration_progress = {"percent": 0, "phase": "Initializing"}
            return
        
        migration_state["structure_done"] = True
        migration_state["results"] = {
            "translation": translation,
            "creation": create_result
        }
        await RunModel.update_status(session["run_id"], "structure_complete", mark_structure_start=False, mark_data_complete=False)
        
        await _set_progress(100, "Structure migration completed")
        migration_state["structure_running"] = False
        
        print("[MIGRATION] Structure migration completed successfully")
    except Exception as e:
        print(f"[MIGRATION] Structure migration error: {e}")
        import traceback
        traceback.print_exc()
        migration_state["structure_running"] = False
        migration_state["structure_done"] = False
        structure_migration_progress = {"percent": 0, "phase": "Initializing"}

# REPLACE the entire @app.post("/api/migrate/structure") function (lines 1715-1881) with:

@app.post("/api/migrate/structure")
async def migrate_structure():
    """
    Kick off structure migration asynchronously so the frontend fetch returns immediately.
    This prevents "Failed to fetch" errors due to long-running AI translations.
    """
    try:
        if migration_state.get("structure_running"):
            return {"ok": False, "message": "Structure migration already running"}

        # Check if structure was already completed (backend restart recovery)
        if migration_state.get("structure_done") and migration_state.get("results"):
            return {"ok": True, "message": "Structure migration already completed", "data": migration_state["results"]}
        
        # Reset state for fresh run
        migration_state["structure_running"] = False
        migration_state["structure_done"] = False
        migration_state["results"] = None
        structure_migration_progress = {"percent": 0, "phase": "Initializing"}

        session = await SessionModel.get_session()
        if not session:
            return {"ok": False, "message": "No active session. Please run Analyze/Extract first."}

        source_id = session.get("source_id")
        target_id = session.get("target_id")
        if not source_id or not target_id:
            return {"ok": False, "message": "Source and target connections must be configured"}

        if not extraction_state.get("done") or not extraction_state.get("results"):
            return {"ok": False, "message": "Please run extraction first before migrating structure"}

        # Mark as running and start background task
        migration_state["structure_running"] = True
        asyncio.create_task(run_structure_migration_task())
        return {"ok": True, "message": "Structure migration started"}
    except Exception as e:
        migration_state["structure_running"] = False
        return {"ok": False, "message": str(e)}
