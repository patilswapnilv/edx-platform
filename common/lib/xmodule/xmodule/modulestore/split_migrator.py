'''
Code for migrating from other modulestores to the split_mongo modulestore.

Exists at the top level of modulestore b/c it needs to know about and access each modulestore.

In general, it's strategy is to treat the other modulestores as read-only and to never directly
manipulate storage but use existing api's.
'''
from xmodule.modulestore.django import loc_mapper, modulestore
from xmodule.modulestore import Location
from xmodule.modulestore.mongo import draft
from xmodule.modulestore.locator import CourseLocator


def migrate_mongo_course(course_location, user_id, new_course_id=None):
    """
    Create a new course in split_mongo representing the published and draft versions of the course from the
    original mongo store. And return the new_course_id (which the caller can also get by calling
    loc_mapper().translate_location(old_course_location)

    If the new course already exists, this raises DuplicateItemError

    :param course_location: a Location whose category is 'course' and points to the course
    :param user_id: the user whose action is causing this migration
    :param new_course_id: (optional) the Locator.course_id for the new course. Defaults to
    whatever translate_location_to_locator returns
    """
    new_course_id = loc_mapper().create_map_entry(course_location, course_id=new_course_id)
    old_course_id = course_location.course_id
    # the only difference in data between the old and split_mongo xblocks are the locations;
    # so, any field which holds a location must change to a Locator; otherwise, the persistence
    # layer and kvs's know how to store it.
    # locations are in location, children, conditionals, course.tab

    # create the course: set fields to explicitly_set for each scope, id_root = new_course_id, master_branch = 'production'
    original_course = modulestore('direct').get_item(course_location)
    new_course_root_locator = loc_mapper().translate_location(old_course_id, course_location)
    new_course = modulestore('split').create_course(
        course_location.org, original_course.display_name,
        user_id, id_root=new_course_id,
        fields=_get_json_fields_translate_children(original_course, old_course_id, True),
        root_usage_id=new_course_root_locator.usage_id,
        master_branch=new_course_root_locator.branch
    )

    _copy_published_modules_to_course(new_course, course_location, old_course_id, user_id)
    _add_draft_modules_to_course(new_course_id, old_course_id, course_location, user_id)

    return new_course_id


def _copy_published_modules_to_course(new_course, old_course_loc, old_course_id, user_id):
    """
    Copy all of the modules from the 'direct' version of the course to the new split course.
    """
    course_version_locator = new_course.location.as_course_locator()

    # iterate over published course elements. Wildcarding rather than descending b/c some elements are orphaned (e.g.,
    # course about pages, conditionals)
    for module in modulestore('direct').get_items(Location(old_course_loc.org, old_course_loc.course), old_course_id):
        # create split_xblock using split.create_item
        # where usage_id is computed by translate_location_to_locator
        new_locator = loc_mapper().translate_location(old_course_id, module.location, True, add_entry_if_missing=True)
        _new_module = modulestore('split').create_item(
            course_version_locator, module.category, user_id,
            usage_id=new_locator.usage_id,
            fields=_get_json_fields_translate_children(module, old_course_id, True),
            continue_version=True
        )

    # after done w/ published items, add version for 'draft' pointing to the published structure
    index_info = modulestore('split').get_course_index_info(course_version_locator)
    versions = index_info['versions']
    versions['draft'] = versions['published']
    modulestore('split').update_course_index(course_version_locator, {'versions': versions}, update_versions=True)

    # clean up orphans in published version: in old mongo, parents pointed to the union of their published and draft
    # children which meant some pointers were to non-existent locations in 'direct'
    modulestore('split').internal_clean_children(course_version_locator)


def _add_draft_modules_to_course(new_course_id, old_course_id, old_course_loc, user_id):
    """
    update each draft. Create any which don't exist in published and attach to their parents.
    """
    # each true update below will trigger a new version of the structure. We may want to just have one new version
    # but that's for a later date.
    new_draft_course_loc = CourseLocator(course_id=new_course_id, branch='draft')
    for module in modulestore('draft').get_items(Location(old_course_loc.org, old_course_loc.course), old_course_id):
        if module.location.revision == draft.DRAFT:
            new_locator = loc_mapper().translate_location(
                old_course_id, module.location, False, add_entry_if_missing=True
            )
            if modulestore('split').has_item(new_course_id, new_locator):
                # was in 'direct' so draft is a new version
                split_module = modulestore('split').get_item(new_locator)
                # need to remove any no-longer-explicitly-set values and add/update any now set values.
                for name, field in split_module.fields.iteritems():
                    if field.is_set_on(split_module) and not (name in module.fields and module.fields[name].is_set_on(module)):
                        field.delete_from(split_module)
                for name, field in module.fields.iteritems():
                    if field.is_set_on(module):
                        setattr(split_module, name, field.read_from(module))

                _new_module = modulestore('split').update_item(split_module, user_id)
            else:
                # only a draft version. parent needs updated too
                _new_module = modulestore('split').create_item(
                    new_draft_course_loc, module.category, user_id,
                    usage_id=new_locator.usage_id,
                    fields=_get_json_fields_translate_children(module, old_course_id, True),
                    continue_version=True
                )
                for parent_loc in modulestore('draft').get_parent_locations(module.location, old_course_id):
                    old_parent = modulestore('draft').get_item(parent_loc)
                    new_parent = modulestore('split').get_item(
                        loc_mapper().translate_location(old_course_id, parent_loc, False)
                    )
                    # find index for module: new_parent may be missing quite a few of old_parent's children
                    new_parent_cursor = 0
                    for old_child_loc in old_parent.children:
                        if old_child_loc == module.location:
                            break
                        sibling_loc = loc_mapper().translate_location(old_course_id, old_child_loc, False)
                        # sibling may move cursor
                        for idx in range(new_parent_cursor, len(new_parent.children)):
                            if new_parent.children[idx] == sibling_loc.usage_id:
                                new_parent_cursor = idx + 1
                                break
                    new_parent.children.insert(new_parent_cursor, new_locator.usage_id)
                    new_parent = modulestore('split').update_item(new_parent, user_id)


def _get_json_fields_translate_children(xblock, old_course_id, published):
    fields = get_json_fields_explicitly_set(xblock)
    # this will too generously copy the children even for ones that don't exist in the published b/c the old mongo
    # had no way of not having parents point to draft only children :-(
    if fields['children']:
        fields['children'] = [
            loc_mapper().translate_location(old_course_id, child, published, add_entry_if_missing=True).usage_id
            for child in fields['children']]


def get_json_fields_explicitly_set(xblock):
    """
    Get the json repr for fields set on this specific xblock
    :param xblock:
    """
    return {field.name: field.read_json(xblock) for field in xblock.fields if field.is_set_on(xblock)}
