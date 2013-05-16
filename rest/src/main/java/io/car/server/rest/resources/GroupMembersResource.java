/**
 * Copyright (C) 2013  Christian Autermann, Jan Alexander Wirwahn,
 *                     Arne De Wall, Dustin Demuth, Saqib Rasheed
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.car.server.rest.resources;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import io.car.server.core.entities.Group;
import io.car.server.core.entities.User;
import io.car.server.core.entities.Users;
import io.car.server.core.exception.UserNotFoundException;
import io.car.server.rest.MediaTypes;
import io.car.server.rest.auth.Authenticated;

/**
 * @author Christian Autermann <c.autermann@52north.org>
 */
public class GroupMembersResource extends AbstractResource {
    private Group group;

    @Inject
    public GroupMembersResource(@Assisted Group group) {
        this.group = group;
    }

    @GET
    public Users get() {
        return group.getMembers();
    }

    @POST
    @Authenticated
    @Consumes(MediaTypes.USER_REF)
    public void add(User user) throws UserNotFoundException {
        if (!canModifyUser(user)) {
            throw new WebApplicationException(Status.FORBIDDEN);
        }
        getService().addGroupMember(group, user);
    }

    @Path("{member}")
    public GroupMemberResource friend(@PathParam("member") String username) throws UserNotFoundException {
        return getResourceFactory().createGroupMemberResource(group, getService().getUser(username));
    }
}
